package toyspark

import scala.collection.mutable.{Set => MutSet, ArrayBuffer}

import java.net._
import java.util.logging.Logger

import org.apache.commons.lang3.SerializationUtils
import toyspark.utilities.{Config, SocketWrapper}
import toyspark.utilities.SocketWrapper._

abstract class SamplingType
final case class RandomSampling(partitionIndex: Int, numPartitions: Int, seed: Int) extends SamplingType
final case class HashSampling(partitionIndex: Int, numPartitions: Int)              extends SamplingType
final case class PairHashSampling(partitionIndex: Int, numPartitions: Int)          extends SamplingType
final case class PairHashSelecting(hashes: List[Int])                               extends SamplingType
final case class FullSampling()                                                     extends SamplingType

abstract class ToySparkMessage                                                 extends Serializable
final case class WorkerHandshake(workerID: Int, dataServerPort: Int)           extends ToySparkMessage
final case class MasterHandshake(dataServerPorts: Array[Int])                  extends ToySparkMessage
final case class DataRequest(targetDatasetID: Int, samplingType: SamplingType) extends ToySparkMessage
final case class DataResponse(payload: List[Any])                              extends ToySparkMessage
final case class StageFinished(nodeID: Int)                                    extends ToySparkMessage
final case class StageFinishedAck()                                            extends ToySparkMessage
final case class ComputationFinished(nodeID: Int)                              extends ToySparkMessage
final case class ComputationFinishedAck()                                      extends ToySparkMessage
final case class CommunicatorShutdown()                                        extends ToySparkMessage

object Communication {
  implicit class CommunicationK(val socket: Socket) {
    def sendToySparkMessage(message: ToySparkMessage): Socket = {
      socket.sendBinaryMessage(SerializationUtils.serialize(message))
      socket
    }
    def recvToySparkMessage(): ToySparkMessage = {
      SerializationUtils.deserialize(socket.recvBinaryMessage()).asInstanceOf[ToySparkMessage]
    }
  }

  def initialize(args: Array[String]): Unit = {
    val config     = Config.fromJsonFile("config.json")
    val thisNodeID = args(0)
    Context.setConfig(config)
    Context.setNodeId(Integer.parseInt(thisNodeID))
    Logger.getGlobal.info("initializing communication with given id " + thisNodeID)

    if (Context.isMaster) {
      masterInitialize(config)
    } else {
      workerInitialize(Context.getNodeId, config)
    }

    // start a communicator to handle data transferring
    new Thread(Communicator()).start()
  }

  def masterInitialize(config: Config): Unit = {
    val (_, masterPort) = config.master
    val numWorkers      = config.workers.length
    var numReadyWorker  = 0

    // set up TCP server, block until all workers are ready
    Logger.getGlobal.info("waiting for workers' connection")
    val masterCtrlServerSocket   = new ServerSocket(masterPort)
    val masterDataServerSocket   = new ServerSocket(0)
    val dataServerPorts          = Array.ofDim[Int](numWorkers + 1)
    val workersCtrlClientSockets = MutSet[Socket]()

    Context.setDataServerSocket(masterDataServerSocket)
    Context.setCtrlServerSocket(masterCtrlServerSocket)

    Logger.getGlobal.info(s"local data server listening on port ${Context.getDataServerSocketPort}")
    dataServerPorts(0) = Context.getDataServerSocketPort

    while (numReadyWorker < numWorkers) {
      val workerCtrlClientSocket = masterCtrlServerSocket.accept()
      val (workerID, dataServerPort) = workerCtrlClientSocket.recvToySparkMessage() match {
        case WorkerHandshake(workerID, dataServerPort) => (workerID, dataServerPort)
        case _                                         => throw new RuntimeException("unexpected message type")
      }

      Logger.getGlobal.info(s"worker $workerID connected, its data server port is $dataServerPort")
      numReadyWorker += 1

      // record its port
      dataServerPorts(workerID) = dataServerPort
      workersCtrlClientSockets.add(workerCtrlClientSocket)
    }

    Context.setDataServerContacts(dataServerPortsToContracts(config, dataServerPorts))

    // tell workers everyone's port
    workersCtrlClientSockets.foreach(socket => {
      socket.sendToySparkMessage(MasterHandshake(dataServerPorts))
      socket.close()
    })

    Logger.getGlobal.info(s"manager contacts: ${Context.getDataServerContacts.mkString(",")}")
  }

  def workerInitialize(workerID: Int, config: Config): Unit = {
    // read master address from config
    val (masterIP, masterPort) = config.master

    // set up sockets
    Context.setDataServerSocket(new ServerSocket(0))

    Logger.getGlobal.info(s"local data server listening on port ${Context.getDataServerSocketPort}")

    // report information to master
    val ctrlClient = new Socket(masterIP, masterPort)
    ctrlClient.sendToySparkMessage(WorkerHandshake(workerID, Context.getDataServerSocketPort))
    ctrlClient.recvToySparkMessage() match {
      case MasterHandshake(dsPorts) => Context.setDataServerContacts(dataServerPortsToContracts(config, dsPorts))
      case _                        => throw new RuntimeException("unexpected message type")
    }
    ctrlClient.close()

    Logger.getGlobal.info(s"data server contacts: ${Context.getDataServerContacts.mkString(",")}")
  }

  def dataServerPortsToContracts(config: Config, dataServerPorts: Array[Int]): Array[InetSocketAddress] = {
    config.getNodesIPs.zip(dataServerPorts).map({ case (ip, port) => new InetSocketAddress(ip, port) }).toArray
  }

  def close(): Unit = {
    if (Context.isMaster) {
      // wait for workers
      var numReadyNodes   = 1
      val incomingSockets = MutSet[Socket]()
      while (numReadyNodes < Context.getNumNodes) {
        val incomingSocket = Context.getCtrlServerSocket.accept()
        incomingSocket.recvToySparkMessage() match {
          case ComputationFinished(nodeID) => Logger.getGlobal.info(s"node $nodeID finished")
          case _                           => throw new RuntimeException("unexpected message type")
        }
        incomingSockets.add(incomingSocket)
        numReadyNodes += 1
      }
      incomingSockets.foreach(socket => {
        socket.sendToySparkMessage(ComputationFinishedAck())
        socket.close()
      })
    } else {
      // tell master we are done
      val (masterAddr, masterPort) = Context.getConfig.master
      val ctrlClientSocket         = new Socket(masterAddr, masterPort)
      ctrlClientSocket.sendToySparkMessage(ComputationFinished(Context.getNodeId)).recvToySparkMessage() match {
        case ComputationFinishedAck() => Logger.getGlobal.info("computing finished ack received")
        case _                        => throw new RuntimeException("unexpected message type")
      }
      ctrlClientSocket.close()
    }

    // tell data server to stop
    val localShutdownSocket = new Socket().connectChaining(Context.getDataServerContacts(Context.getNodeId))
    localShutdownSocket.sendToySparkMessage(CommunicatorShutdown())
    localShutdownSocket.close()
  }
}
