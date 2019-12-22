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
        case CommunicatorShutdown() => shouldShutdown = true
        case _                      => throw new RuntimeException("unexpected message type")
      }
      incomingSocket.close()
    }
    Context.getDataServerSocket.close()
  }

  private def sendRequestedData(incomingSocket: Socket, targetDatasetID: Int, samplingType: SamplingType): Unit = {
    def applySampling(entry: List[_], partitionIndex: Int, numPartitions: Int, seed: Int): List[_] = {
      val selectedIndices =
        new Random(seed).shuffle(entry.indices.toList).evenlyPartitioned(numPartitions).toVector(partitionIndex)
      selectedIndices.map(idx => entry(idx))
    }

    // todo new thread
    val entries = Context.getSendingBufferDataByDatasetID(targetDatasetID)
    val resp = samplingType match {
      case FullSampling() =>
        DataResponse(entries.flatten)
      case PartialSampling(partitionIndex, numPartitions, seed) =>
        DataResponse(entries.flatMap(entry => applySampling(entry, partitionIndex, numPartitions, seed)))
    }
    incomingSocket.sendToySparkMessage(resp)
  }
}
