package toyspark
import scala.collection.mutable.ArrayBuffer
import math.{ceil, min}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.CyclicBarrier

import org.apache.commons.lang3.SerializationUtils
import toyspark.utilities.HDFSUtil
import toyspark.utilities.SocketWrapper._
import toyspark.Communication._
import TypeAliases._
import java.nio.file.Paths
import java.util.logging.Logger
import toyspark.utilities.Extensions._

final case class Executor(datasets: List[Dataset[_]],
                          executorId: Int,
                          thisStagePartitions: PartitionSchema,
                          barrier: CyclicBarrier,
                          hdfsBarrier: CyclicBarrier)
    extends Runnable {
  private def makeList[T](elems: Object, dtype: T) = elems.asInstanceOf[List[T]]

  override def run(): Unit = {
    val initialData = getInitialData(datasets.head)
    val finalData   = datasets.tail.foldLeft(initialData)((iter, trans) => performTransformation(trans, iter))
    val fetchedDatasetID = datasets.last match {
      case MemCacheDataset(wrapping) => Context.cread(wrapping)
      case _                         => Context.cread(datasets.last)
    }
    Context.setSendingBufferEntry(fetchedDatasetID, executorId, finalData)
    barrier.await()
  }

  private def getExecutorIndex: Int               = executorId + thisStagePartitions.take(Context.getNodeId).sum
  private def getSeed(datasets: Dataset[_]*): Int = datasets.toList.hashCode()
  private def saveToMemCache(dataset: Dataset[_], data: List[_]): Unit = {
    val datasetID = Context.cread(dataset)
    if (Context.hasMemCacheMark(datasetID) && !Context.hasMemCacheEntry(datasetID, executorId)) {
      Context.setMemCacheEntry(datasetID, executorId, data)
    }
  }

  private def getInitialData(headDataset: Dataset[_]): List[_] = {
    val ret = headDataset match {
      case MemCacheDataset(wrapping) => Context.getMemCacheEntry(Context.cread(wrapping), executorId)
      case GeneratedDataset(_, generator) =>
        generator(Context.getNodeId, executorId)
      case ReadDataset(partitions, dataFile, dtype) =>
        // todo: Need optimization with less access to hdfs file
        val hdfsData =
          makeList(SerializationUtils.deserialize(HDFSUtil.readHDFSFile(dataFile, Some(hdfsBarrier))), dtype)
        val threads = partitions.sum
        var index   = executorId
        for (i <- 0 until Context.getNodeId) {
          index += partitions(i)
        }
        val len    = hdfsData.toArray.length
        val slices = ceil(len.asInstanceOf[Double] / threads).asInstanceOf[Int]
        hdfsData.slice(index * slices, min(len, (index + 1) * slices))
      case RepartitionDataset(ups, _) =>
        val seed = getSeed(headDataset, ups)
        requestDataOverNetwork(ups, RandomSampling(getExecutorIndex, thisStagePartitions.sum, seed))
      case HashShuffleDataset(ups) =>
        requestDataOverNetwork(ups, HashSampling(getExecutorIndex, thisStagePartitions.sum))
      case PairHashShuffleDataset(ups) =>
        requestDataOverNetwork(ups, PairHashSampling(getExecutorIndex, thisStagePartitions.sum))
      case _ => throw new RuntimeException("unexpected initial dataset")
    }

    saveToMemCache(headDataset, ret)
    ret
  }

  private def performTransformation(transformationToPerform: Dataset[_], data: List[_]): List[_] = {
    val ret = transformationToPerform match {
      case MappedDataset(_, mapper)       => data.map(mapper.asInstanceOf[Any => Any])
      case FlatMappedDataset(_, mapper)   => data.flatMap(mapper.asInstanceOf[Any => Iterable[Any]])
      case LocalDistinctDataset(_)        => data.distinct
      case LocalGroupedByKeyDataset(_)   => data.asInstanceOf[List[(Any, Any)]].groupByKey()
      case LocalReducedByKeyDataset(_, r) => data.asInstanceOf[List[(Any, Any)]].reduceByKey(r)
      case FilteredDataset(_, pred)       => data.filter(pred.asInstanceOf[Any => Boolean])
      case LocalCountDataset(_)           => List(data.length)
      case LocalReduceDataset(_, reducer) => List(data.reduce(reducer.asInstanceOf[(Any, Any) => Any]))
      case UnionDataset(lhs, rhs) =>
        val seed = getSeed(lhs, rhs, transformationToPerform)
        data ++ requestDataOverNetwork(rhs, RandomSampling(getExecutorIndex, thisStagePartitions.sum, seed))
      case IntersectionDataset(_, rhs) =>
        val rhsData = requestDataOverNetwork(rhs, FullSampling())
        data.filter(x => rhsData.contains(x))
      case CartesianDataset(_, rhs) =>
        val rhsData = requestDataOverNetwork(rhs, FullSampling())
        data.flatMap(x => rhsData.map(y => (x, y)))
      case IsSavingSeqFileOkDataset(_, dir, name) =>
        val serial = SerializationUtils.serialize(data)
        List(
          HDFSUtil.createNewHDFSFile(
            Paths.get(dir, name + Context.getNodeId + executorId + ".dat").toString,
            serial,
            Some(hdfsBarrier)
          ))
      case _ =>
        throw new RuntimeException("unexpected intermediate or final transformation")
    }

    saveToMemCache(transformationToPerform, ret)
    ret
  }

  private def requestDataOverNetwork(targetDataset: Dataset[_], samplingType: SamplingType): List[_] = {
    val arrayBuffer     = new ArrayBuffer[Any]()
    val targetDatasetID = Context.cread(targetDataset)

    for (contact <- Context.getDataServerContacts) {
      Logger.getGlobal.info(s"requesting data from $contact, target dataset $targetDatasetID")
      val socket = new Socket().connectChaining(contact)
      socket.sendToySparkMessage(DataRequest(targetDatasetID, samplingType)).recvToySparkMessage() match {
        case DataResponse(payload) => arrayBuffer.appendAll(payload)
        case _                     => throw new RuntimeException("unexpected response")
      }
      socket.close()
      Logger.getGlobal.info(s"received data from $contact, target dataset $targetDatasetID")
    }
    arrayBuffer.toList
  }
}
