package toyspark

import java.net._
import java.util.logging.Logger

import org.apache.commons.lang3.SerializationUtils
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import toyspark.utilities.{Config, SocketWrapper}
import toyspark.utilities.SocketWrapper._

object Communication {
  def initialize(args: Array[String]): Unit = {
    val config                 = Config.fromJsonFile("config.json")
    val (masterIp, masterPort) = config.master
    val myId                   = args(0)
    Context.setConfig(config)
    Context.setNodeId(Integer.parseInt(myId))
    Logger.getGlobal.info("initializing communication with id " + myId)

    // if master, listen
    // if worker, report
    if (myId == "0") {
      // id is 0, this is master
      val numWorkers     = config.workers.length
      val ready          = Array.fill(numWorkers)(false)
      var numReadyWorker = 0
      var allReady       = false

      // set up TCP server, block until all workers are ready
      Logger.getGlobal.info("waiting for workers' connection")
      val server = new ServerSocket(masterPort)
      val ports  = Array.ofDim[Int](numWorkers + 1)
      ports(0) = masterPort

      Context.setManagerServerSocket(server)
      while (!allReady) {
        val socket         = server.accept()
        val message        = parse(SocketWrapper.recvTextMessage(socket))
        val List(port)     = for { JInt(port) <- message \ "port" } yield port.toInt
        val List(workerId) = for { JInt(port) <- message \ "id" } yield port.toInt

        // workers' id starts from 1, so minus 1
        val workerIdx = workerId - 1
        if (!ready(workerIdx)) {
          ready(workerIdx) = true
          Logger.getGlobal.info("worker " + workerId + " connected")
          numReadyWorker += 1
          allReady ||= numReadyWorker == numWorkers
        }

        // record its port
        ports(workerId) = port
        socket.close()
      }

      Context.setManagerContacts(ports)

      // tell workers everyone's port
      val contactsMessage = SerializationUtils.serialize(ports)
      for ((workerIp, i) <- config.workers.view.zipWithIndex) {
        val socket = new Socket(workerIp, ports(i + 1))
        sendBinaryMessage(socket, contactsMessage)
        socket.close()
      }

      Logger.getGlobal.info(s"manager contacts: ${Context.getManagerContacts.mkString(",")}")
    } else {
      // set up workers server socket
      Context.setManagerServerSocket(new ServerSocket(0))

      // id is not 0, this is a worker, report ready to master
      val sendingSocket = new Socket(masterIp, masterPort)
      val message       = compact(render(("id" -> Integer.parseInt(myId)) ~ ("port" -> Context.getManagerServerPort)))
      SocketWrapper.sendTextMessage(sendingSocket, message)
      sendingSocket.close()

      // waiting for master's broadcast
      val receivingSocket = Context.getManagerServerSocket.accept()
      val contactsMessage = recvBinaryMessage(receivingSocket)
      Context.setManagerContacts(SerializationUtils.deserialize(contactsMessage).asInstanceOf[Array[Int]])
      receivingSocket.close()

      Logger.getGlobal.info(s"manager contacts: ${Context.getManagerContacts.mkString(",")}")
    }
  }
}
