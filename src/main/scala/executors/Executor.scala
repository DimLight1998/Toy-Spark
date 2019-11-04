package executors

import java.net.ServerSocket
import java.util.concurrent.CyclicBarrier
import java.util.logging.Logger

import Communication.{Context, SocketWrapper}
import Datasets._
import org.apache.commons.lang3.SerializationUtils

final case class Executor(datasets: List[Dataset[_]],
                          executorId: Int,
                          nodeId: Int,
                          downstreamPartitions: PartitionSchema,
                          barrier: CyclicBarrier)
    extends Runnable {
  override def run(): Unit = {
    // execute the stage
    val headDataset :: tailDatasets = datasets

    // get data from the first dataset
    val initialData: List[_] = headDataset match {
      case GeneratedDataset(_, generator) => generator(nodeId, executorId)
      case CoalescedDataset(upstream, _) => {
        // fetch data from each upstream node
        // todo #9 [H] get data from upstream
        ???
      }
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
        case FilteredDataset(_, pred)       => current = current.filter(pred.asInstanceOf[Any => Boolean])
        case MappedDataset(_, mapper)       => current = current.map(mapper.asInstanceOf[Any => Any])
        case LocalReduceDataset(_, reducer) => current = List(current.reduce(reducer.asInstanceOf[(Any, Any) => Any]))
        case _                              => throw new RuntimeException("unexpected intermediate transformation")
      }
    }

    // wait for the downstream executors to fetch data
    println(s"thread $executorId at node $nodeId has ${current.length} records")

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
      val message                  = SocketWrapper.extractBinaryMessage(socket)
      val (dsNodeId, dsExecutorId) = SerializationUtils.deserialize(message).asInstanceOf[(Int, Int)]
      // todo #8 [H] encode and decode to know which executor is it
      val downstreamIndex = downstreamPartitions.take(dsNodeId).sum + dsExecutorId

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
