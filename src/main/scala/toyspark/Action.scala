package toyspark

import scala.collection.mutable.ArrayBuffer
import java.net.Socket
import java.nio.file.Paths
import java.util.concurrent.CyclicBarrier
import java.util.logging.Logger

import utilities._
import StageSplit._
import Communication._
import SocketWrapper._
import org.apache.commons.lang3.SerializationUtils
import toyspark.TypeAliases.Stage

abstract class Action[T] {
  def performOnly(): Dataset[_] = {
    // split stages then ensure every dataset in stages has an ID
    def ensureHavingIDs(stage: Stage): Unit = stage._1.foreach(dataset => Context.cread(dataset))

    val (stages, _) = splitStagesDAG(this, substituteCached = false)
    stages.foreach(ensureHavingIDs)
    val (substitutedStages, fetchTargetDataset) = splitStagesDAG(this, substituteCached = true)
    substitutedStages.foreach(ensureHavingIDs)

    // perform stages
    for (i <- substitutedStages.indices) {
      performStage(substitutedStages(i), i)
    }

    // return the dataset for final fetching
    fetchTargetDataset
  }

  def performAndFetchFinal(): Vector[T] = {
    val fetchTargetDataset = performOnly()
    fetchFinalArrayBuffer(fetchTargetDataset)
  }

  private def performStage(stage: Stage, iDebug: Int): Unit = {
    val (datasets, partitions) = stage
    val numLocalPartition      = partitions(Context.getNodeId)
    val barrier                = new CyclicBarrier(numLocalPartition + 1)
    val hdfsBarrier            = new CyclicBarrier(numLocalPartition)

    val threads =
      (0 until numLocalPartition).map(i => new Thread(Executor(datasets, i, partitions, barrier, hdfsBarrier)))
    threads.foreach(t => t.start())

    Logger.getGlobal.info(s"stage $iDebug started...")
    barrier.await()
    Logger.getGlobal.info(s"stage $iDebug finished locally, reporting to master...")

    if (Context.isMaster) {
      // receive finishing message from workers, then echo
      var numReported     = 1                     // include myself
      val incomingSockets = ArrayBuffer[Socket]() // save incoming sockets
      while (numReported < Context.getNumNodes) {
        val incomingSocket = Context.getCtrlServerSocket.accept()
        val message        = incomingSocket.recvToySparkMessage()
        message match {
          case StageFinished(nodeID) =>
            Logger.getGlobal.info(s"node $nodeID finished stage $iDebug")
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
        case StageFinishedAck() => Logger.getGlobal.info(s"stage $iDebug finished ACK received")
        case _                  => throw new RuntimeException("unexpected message type")
      }
      socket.close()
    }
  }

  private def fetchFinalArrayBuffer(targetDataset: Dataset[_]): Vector[T] = {
    // todo move
    def requestDataOverNetwork(targetDataset: Dataset[_], samplingType: SamplingType): List[_] = {
      val arrayBuffer     = new ArrayBuffer[Any]()
      val targetDatasetID = Context.cread(targetDataset)

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

case class SaveAsSingleFileAction[T](upstream: Dataset[T], dir: String, name: String) extends Action[Boolean] {
  def perform(): Boolean = {
    if (Context.isMaster) {
      val data = performAndFetchFinal().toList
      val serial = SerializationUtils.serialize(data)
      HDFSUtil.createNewHDFSFile(Paths.get(dir, name).toString, serial)
      true
    } else {
      performOnly()
      false
    }
  }
}