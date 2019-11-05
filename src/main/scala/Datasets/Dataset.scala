package Datasets

import java.net.Socket
import java.util.concurrent.CyclicBarrier
import java.util.logging.Logger

import Communication.SocketWrapper.sendBinaryMessage
import Communication.{Config, Context, SocketWrapper}
import executors.Executor
import org.apache.commons.lang3.SerializationUtils

import scala.collection.mutable.ArrayBuffer

abstract class Dataset[T] {
  def map[U](mapper: T => U): MappedDataset[T, U]           = MappedDataset(this, mapper)
  def filter(pred: T => Boolean): FilteredDataset[T]        = FilteredDataset(this, pred)
  def coalesced(partitions: List[Int]): CoalescedDataset[T] = CoalescedDataset(this, partitions)
  def reduce(reducer: (T, T) => T, workerRet: T): T         = ReduceAction(this, reducer, workerRet).perform()
  def collect(workerRet: List[T]): List[T]                  = CollectAction(this, workerRet).perform()
}

object Dataset {
  def generate[T](partitions: List[Int], generator: (Int, Int) => List[T]): GeneratedDataset[T] =
    GeneratedDataset(partitions, generator)
}

case class GeneratedDataset[T](partitions: List[Int], generator: (Int, Int) => List[T]) extends Dataset[T]
case class MappedDataset[T, U](upstream: Dataset[T], mapper: T => U)                    extends Dataset[U]
case class FilteredDataset[T](upstream: Dataset[T], pred: T => Boolean)                 extends Dataset[T]
case class CoalescedDataset[T](upstream: Dataset[T], partitions: List[Int])             extends Dataset[T]
case class LocalReduceDataset[T](upstream: Dataset[T], reducer: (T, T) => T)            extends Dataset[T]

abstract class Action[T] {
  def performTransformations(): Unit = {
    val stages = splitStages(this).toArray

    // execute stages in the topological order
    for (i <- stages.indices) {
      val (datasets, partitions) = stages(i)
      val (_, downstreamPartitions) =
        if (i == stages.length - 1) { (null, 1 :: List.fill(Context.getNumNodes - 1)(0)) } else stages(i + 1)
      val numLocalPartition = partitions(Context.getNodeId)
      val barrier           = new CyclicBarrier(numLocalPartition + 1)

      Context.setLocalExecutorServerPorts(Array.ofDim(numLocalPartition))
      val threads =
        (0 until numLocalPartition).map(i => new Thread(Executor(datasets, i, downstreamPartitions, barrier)))
      threads.foreach(t => t.start())

      barrier.await()

      // send executor ports
      Logger.getGlobal.info(s"local executor server ports: ${Context.getLocalExecutorServerPorts.mkString(",")}")

      if (Context.isMaster) {
        val allExecutorServerPorts = Array.ofDim[Array[Int]](Context.getNumNodes)
        allExecutorServerPorts(0) = Context.getLocalExecutorServerPorts

        var numReported = 1
        while (numReported < Context.getNumNodes) {
          val socket  = Context.getManagerServerSocket.accept()
          val message = SocketWrapper.extractBinaryMessage(socket)
          val (senderNodeId, executorServerPorts) =
            SerializationUtils.deserialize(message).asInstanceOf[(Int, Array[Int])]
          allExecutorServerPorts(senderNodeId) = executorServerPorts
          numReported += 1
          socket.close()
        }

        Context.setAllExecutorServerPorts(allExecutorServerPorts)
        val allExecutorServerPortsMessage = SerializationUtils.serialize(allExecutorServerPorts)
        for ((workerIp, i) <- Context.getConfig.workers.view.zipWithIndex) {
          val ports  = Context.getManagerContacts
          val socket = new Socket(workerIp, ports(i + 1))
          sendBinaryMessage(socket, allExecutorServerPortsMessage)
          socket.close()
        }

      } else {
        val (masterAddr, masterPort) = Context.getConfig.master
        val socket                   = new Socket(masterAddr, masterPort)
        val executorsServerPorts =
          SerializationUtils.serialize((Context.getNodeId, Context.getLocalExecutorServerPorts))
        SocketWrapper.sendBinaryMessage(socket, executorsServerPorts)
        socket.close()

        val receivingSocket               = Context.getManagerServerSocket.accept()
        val allExecutorServerPortsMessage = SocketWrapper.extractBinaryMessage(receivingSocket)
        Context.setAllExecutorServerPorts(
          SerializationUtils.deserialize(allExecutorServerPortsMessage).asInstanceOf[Array[Array[Int]]]
        )
      }

      Logger.getGlobal.info(
        s"all executor server ports: ${(Context.getAllExecutorServerPorts map { _.mkString(",") }).mkString("; ")}"
      )
    }
  }

  def fetchFinalArrayBuffer(): ArrayBuffer[T] = {
    val arrayBuffer                      = new ArrayBuffer[T]
    val Config((masterIp, _), workerIps) = Context.getConfig
    val allIps                           = masterIp :: workerIps
    val allEndpoints = allIps zip Context.getAllExecutorServerPorts flatMap {
      case (ip, ports) => ports map { (ip, _) }
    }
    for ((ip, port) <- allEndpoints) {
      val socket  = new Socket(ip, port)
      val message = SerializationUtils.serialize((Context.getNodeId, 0))
      SocketWrapper.sendBinaryMessage(socket, message)
      val recv = SocketWrapper.extractBinaryMessage(socket)
      arrayBuffer.appendAll(SerializationUtils.deserialize(recv).asInstanceOf[List[T]])
      socket.close()
    }

    arrayBuffer
  }
}

case class ReduceAction[T](upstream: Dataset[T], reducer: (T, T) => T, workerRet: T) extends Action[T] {
  def perform(): T = {
    performTransformations()

    // collect data from upstream executors
    if (Context.isMaster) {
      fetchFinalArrayBuffer().reduce(reducer)
    } else {
      workerRet
    }
  }
}

case class CollectAction[T](upstream: Dataset[T], workerRet: List[T]) extends Action[T] {
  def perform(): List[T] = {
    performTransformations()

    // collect data from upstream executors
    if (Context.isMaster) {
      fetchFinalArrayBuffer().toList
    } else {
      workerRet
    }
  }
}
