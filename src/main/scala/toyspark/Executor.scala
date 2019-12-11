package toyspark
import scala.collection.mutable.ArrayBuffer
import math.{ceil, min}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.CyclicBarrier
import org.apache.commons.lang3.SerializationUtils
import toyspark.utilities.HDFSUtil
import utilities._
import TypeAliases._
import java.nio.file.Paths

final case class Executor(datasets: List[Dataset[_]],
                          executorId: Int,
                          downstreamPartitions: PartitionSchema,
                          barrier: CyclicBarrier,
                          innerBarrier: CyclicBarrier)
    extends Runnable {
  private def makeList[T](elems: Object, dtype: T) = elems.asInstanceOf[List[T]]
  override def run(): Unit = {
    // execute the stage
    val headDataset :: tailDatasets = datasets
    // get data from the first dataset
    val initialData: List[_] = headDataset match {
      case GeneratedDataset(_, generator) =>
        generator(Context.getNodeId, executorId)
      case ReadDataset(partitions, dataFile, dtype) =>
        // todo: Need optimization with less access to hdfs file
        val datas   = makeList(SerializationUtils.deserialize(HDFSUtil.readHDFSFile(dataFile, Some(innerBarrier))), dtype)
        val threads = partitions.sum
        var index   = executorId
        for (i <- 0 until Context.getNodeId) {
          index += partitions(i)
        }
        val len    = datas.toArray.length
        val slices = ceil(len.asInstanceOf[Double] / threads).asInstanceOf[Int]
        datas.slice(index * slices, min(len, (index + 1) * slices))
      case CoalescedDataset(_, _) =>
        val arrayBuffer                      = new ArrayBuffer[Any]
        val Config((masterIp, _), workerIps) = Context.getConfig
        val allIps                           = masterIp :: workerIps
        val allEndpoints = allIps zip Context.getAllExecutorServerPorts flatMap {
          case (ip, ports) => ports map { (ip, _) }
        }
        for ((ip, port) <- allEndpoints) {
          val socket  = new Socket(ip, port)
          val message = SerializationUtils.serialize((Context.getNodeId, executorId))
          SocketWrapper.sendBinaryMessage(socket, message)
          val recv = SocketWrapper.recvBinaryMessage(socket)
          arrayBuffer.appendAll(SerializationUtils.deserialize(recv).asInstanceOf[List[Any]])
          socket.close()
        }
        arrayBuffer.toList
      case _ => throw new RuntimeException("unexpected first dataset")
    }

    // initialize executors' server socket
    val server = new ServerSocket(0)
    Context.getLocalExecutorServerPorts(executorId) = server.getLocalPort

    // notify manager thread that executor server sockets initialized
    barrier.await()
    // for each record in it, compute them dataset by dataset
    var current = initialData
    for (transformation <- tailDatasets) {
      transformation match {
        // todo #4 [L] how to cooperate with type system
        case FilteredDataset(_, pred) =>
          current = current.filter(pred.asInstanceOf[Any => Boolean])
        case MappedDataset(_, mapper) =>
          current = current.map(mapper.asInstanceOf[Any => Any])
        case LocalCountDataset(_) =>
          current = List(current.length)
        case LocalReduceDataset(_, reducer) =>
          current = List(current.reduce(reducer.asInstanceOf[(Any, Any) => Any]))
        case LocalSaveAsSequenceFileDataset(_, dir, name) =>
          val serial = SerializationUtils.serialize(current)
          println(
            "nodeid=%d,executorid=%d,path=%s".format(
              Context.getNodeId,
              executorId,
              Paths.get(dir, name + Context.getNodeId + executorId + ".dat").toString))
          current = List(
            HDFSUtil.createNewHDFSFile(
              Paths.get(dir, name + Context.getNodeId + executorId + ".dat").toString,
              serial,
              Some(innerBarrier)
            ))
        case _ =>
          throw new RuntimeException("unexpected intermediate transformation")
      }
    }

    // wait for the downstream executors to fetch data
    println(s"thread $executorId at node ${Context.getNodeId} has ${current.length} records")

    // todo #12 [L] better partition algorithm
    val numDownstreamExecutor = downstreamPartitions.sum
    val sliced = if (current.length % numDownstreamExecutor == 0) {
      current.grouped(current.length / numDownstreamExecutor)
    } else {
      current.grouped(current.length / numDownstreamExecutor + 1)
    }

    val packed = sliced.toArray map { SerializationUtils.serialize(_) }

    // set up the tcp server
    val fetched              = Array.fill(numDownstreamExecutor)(false)
    var numFetchedDownstream = 0
    var allFetched           = false

    while (!allFetched) {
      val socket                   = server.accept()
      val message                  = SocketWrapper.recvBinaryMessage(socket)
      val (dsNodeId, dsExecutorId) = SerializationUtils.deserialize(message).asInstanceOf[(Int, Int)]
      val downstreamIndex          = downstreamPartitions.take(dsNodeId).sum + dsExecutorId

      val data = packed(downstreamIndex)
      SocketWrapper.sendBinaryMessage(socket, data)
      if (!fetched(downstreamIndex)) {
        fetched(downstreamIndex) = true
        numFetchedDownstream += 1
        allFetched ||= numFetchedDownstream == numDownstreamExecutor
      }
      socket.close()
    }

    // now all downstream executors get their data, can return now
  }
}
