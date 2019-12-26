package toyspark

import java.net.Socket
import java.util.logging.Logger

import Communication._

import scala.util.Random
import toyspark.utilities.Extensions._

final case class Communicator() extends Runnable {
  override def run(): Unit = {
    var shouldShutdown = false
    while (!shouldShutdown) {
      val incomingSocket = Context.getDataServerSocket.accept()
      incomingSocket.recvToySparkMessage() match {
        case DataRequest(targetDatasetID, samplingType) =>
          Logger.getGlobal.info(s"received data request for dataset $targetDatasetID")
          sendRequestedData(incomingSocket, targetDatasetID, samplingType)
        case CommunicatorShutdown() =>
          shouldShutdown = true
          incomingSocket.close()
        case _ => throw new RuntimeException("unexpected message type")
      }
    }
    Context.getDataServerSocket.close()
  }

  private def sendRequestedData(incomingSocket: Socket, targetDatasetID: Int, samplingType: SamplingType): Unit = {
    def applyRandomSampling(entry: List[_], partitionIndex: Int, numPartitions: Int, seed: Int): List[_] = {
      val selectedIndices =
        new Random(seed).shuffle(entry.indices.toList).evenlyPartitioned(numPartitions).toVector(partitionIndex)
      selectedIndices.map(idx => entry(idx))
    }

    def applyHashSampling(entry: List[_], partitionIndex: Int, numPartitions: Int): List[_] = {
      entry.filter(e => e.hashCode() % numPartitions == partitionIndex)
    }

    new Thread {
      override def run(): Unit = {
        val entries = Context.getSendingBufferDataByDatasetID(targetDatasetID)
        val resp = samplingType match {
          case FullSampling() =>
            DataResponse(entries.flatten)
          case RandomSampling(partitionIndex, numPartitions, seed) =>
            DataResponse(entries.flatMap(entry => applyRandomSampling(entry, partitionIndex, numPartitions, seed)))
          case HashSampling(partitionIndex, numParittions) =>
            DataResponse(entries.flatMap(entry => applyHashSampling(entry, partitionIndex, numParittions)))
        }
        incomingSocket.sendToySparkMessage(resp)
        incomingSocket.close()
      }
    }.start()
  }
}
