package toyspark

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import java.net.Socket
import java.util.concurrent.CyclicBarrier
import java.util.logging.Logger

import utilities._
import StageSplit._
import Communication._
import SocketWrapper._
import toyspark.TypeAliases.Stage

abstract class Action[T] {
  def performOnly(): Dataset[_] = {
    // split stages then ensure every dataset in stages has an ID
    val (stages, fetchTargetDataset) = splitStagesDAG(this)
    stages.foreach({ case (datasets, _) => datasets.foreach(dataset => Context.getOrAssignDatasetID(dataset)) })
    // performStages
    performStages(stages)
    fetchTargetDataset
  }

  def performAndFetchFinal(): Vector[T] = {
    val fetchTargetDataset = performOnly()
    fetchFinalArrayBuffer(fetchTargetDataset)
  }

  private def performStages(stages: List[Stage]): Unit = {
    // start a communicator to handle data transferring
    new Thread(Communicator()).start()

    for (i <- stages.indices) {
      val (datasets, partitions) = stages(i)
      val numLocalPartition      = partitions(Context.getNodeId)
      val barrier                = new CyclicBarrier(numLocalPartition + 1)
      val hdfsBarrier            = new CyclicBarrier(numLocalPartition)

      val threads =
        (0 until numLocalPartition).map(i => new Thread(Executor(datasets, i, partitions, barrier, hdfsBarrier)))
      threads.foreach(t => t.start())

      Logger.getGlobal.info(s"stage $i started...")
      barrier.await()
      Logger.getGlobal.info(s"stage $i finished locally, reporting to master...")

      if (Context.isMaster) {
        // receive finishing message from workers, then echo
        var numReported     = 1                     // include myself
        val incomingSockets = ArrayBuffer[Socket]() // save incoming sockets
        while (numReported < Context.getNumNodes) {
          val incomingSocket = Context.getCtrlServerSocket.accept()
          val message        = incomingSocket.recvToySparkMessage()
          message match {
            case StageFinished(nodeID) =>
              Logger.getGlobal.info(s"node $nodeID finished stage $i")
              numReported += 1
              incomingSockets += incomingSocket
            case _ => throw new RuntimeException("unexpected message type")
          }
        }

        for (incomingSocket <- incomingSockets) {
          incomingSocket.sendToySparkMessage(StageFinishedAck())
          incomingSocket.close()
        }
      } else {
        // send a message to master that we are finished
        val (masterAddr, masterPort) = Context.getConfig.master
        val socket                   = new Socket(masterAddr, masterPort)
        socket.sendToySparkMessage(StageFinished(Context.getNodeId))
        socket.recvToySparkMessage() match {
          case StageFinishedAck() => Logger.getGlobal.info(s"stage $i finished ACK received")
          case _                  => throw new RuntimeException("unexpected message type")
        }
        socket.close()
      }
    }
  }

  private def fetchFinalArrayBuffer(targetDataset: Dataset[_]): Vector[T] = {
    // todo move
    def requestDataOverNetwork(targetDataset: Dataset[_], samplingType: SamplingType): List[_] = {
      val arrayBuffer     = new ArrayBuffer[Any]()
      val targetDatasetID = Context.getOrAssignDatasetID(targetDataset)

      for (contact <- Context.getDataServerContacts) {
        val socket = new Socket().connectChaining(contact)
        socket.sendToySparkMessage(DataRequest(targetDatasetID, samplingType)).recvToySparkMessage() match {
          case DataResponse(payload) => arrayBuffer.appendAll(payload)
          case _                     => throw new RuntimeException("unexpected response")
        }
        socket.close()
      }
      arrayBuffer.toList
    }

    val data = requestDataOverNetwork(targetDataset, FullSampling()).asInstanceOf[List[T]]
    data.toVector
  }
}

case class ReduceAction[T](upstream: Dataset[T], reducer: (T, T) => T, workerRet: T) extends Action[T] {
  def perform(): T = {
    // collect data from upstream executors
    if (Context.isMaster) {
      performAndFetchFinal().reduce(reducer)
    } else {
      performOnly()
      workerRet
    }
  }
}

case class CollectAction[T](upstream: Dataset[T], workerRet: List[T]) extends Action[T] {
  def perform(): List[T] = {
    if (Context.isMaster) {
      performAndFetchFinal().toList
    } else {
      performOnly()
      workerRet
    }
  }
}

case class CountAction[T](upstream: Dataset[T]) extends Action[Int] {
  def perform(): Int = {
    if (Context.isMaster) {
      performAndFetchFinal().sum
    } else {
      performOnly()
      0
    }
  }
}

case class TakeAction[T](upstream: Dataset[T], count: Int, workerRet: List[T]) extends Action[T] {
  def perform(): List[T] = {
    // todo: Take action should only collect 'count' elems to master
    if (Context.isMaster) {
      performAndFetchFinal().toList.slice(0, count)
    } else {
      performOnly()
      workerRet
    }
  }
}

case class SaveAsSequenceFileAction[T](upstream: Dataset[T], dir: String, name: String) extends Action[Boolean] {
  def perform(): Boolean = {
    if (Context.isMaster) {
      performAndFetchFinal().toList.reduce(_ && _)
    } else {
      performOnly()
      false
    }
  }
}
