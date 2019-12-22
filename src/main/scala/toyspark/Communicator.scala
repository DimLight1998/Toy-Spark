package toyspark

import java.net.Socket
import java.util.logging.Logger

import Communication._

import scala.util.Random
import toyspark.utilities.Extensions._

final case class Communicator() extends Runnable {
  override def run(): Unit = {
    while (true) {
      val incomingSocket = Context.getDataServerSocket.accept()
      incomingSocket.recvToySparkMessage() match {
        case DataRequest(targetDatasetID, samplingType) =>
          Logger.getGlobal.info(s"received data request for dataset $targetDatasetID")
          sendRequestedData(incomingSocket, targetDatasetID, samplingType)
          incomingSocket.close()
        case _ => throw new RuntimeException("unexpected message type")
      }
    }
  }

  private def sendRequestedData(incomingSocket: Socket, targetDatasetID: Int, samplingType: SamplingType): Unit = {
    def applySampling(entry: List[_], partitionIndex: Int, numPartitions: Int, seed: Int): List[_] = {
      val selectedIndices =
        new Random(seed).shuffle(entry.indices.toList).evenlyPartitioned(numPartitions).toVector(partitionIndex)
      selectedIndices.map(idx => entry(idx))
    }

    // todo new thread
    val entries = Context.getSendingBufferDataByDatasetID(targetDatasetID).toArray
    val resp = samplingType match {
      case FullSampling() =>
        DataResponse(entries.toList.flatten)
      case PartialSampling(partitionIndex, numPartitions, seed) =>
        DataResponse(entries.flatMap(entry => applySampling(entry, partitionIndex, numPartitions, seed)).toList)
    }
    incomingSocket.sendToySparkMessage(resp)
  }
}
