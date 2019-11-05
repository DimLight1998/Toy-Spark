package toyspark

import java.net._

import toyspark.utilities.Config

object Context {
  private var _nodeId: Int                               = _
  private var _managerServerSocket: ServerSocket         = _
  private var _managerContacts: Array[Int]               = _
  private var _localExecutorServerPorts: Array[Int]      = _
  private var _allExecutorServerPorts: Array[Array[Int]] = _
  private var _config: Config                            = _

  def setNodeId(nodeId: Int): Unit = {
    _nodeId = nodeId
  }

  def getNodeId: Int = _nodeId

  def isMaster: Boolean = _nodeId == 0

  def setManagerServerSocket(serverSocket: ServerSocket): Unit = {
    _managerServerSocket = serverSocket
  }

  def getManagerServerSocket: ServerSocket = _managerServerSocket

  def getManagerServerPort: Int = _managerServerSocket.getLocalPort

  def setManagerContacts(contacts: Array[Int]): Unit = {
    _managerContacts = contacts
  }

  def getManagerContacts: Array[Int] = _managerContacts

  def setLocalExecutorServerPorts(ports: Array[Int]): Unit = {
    _localExecutorServerPorts = ports
  }

  def getLocalExecutorServerPorts: Array[Int] = _localExecutorServerPorts

  def setAllExecutorServerPorts(ports: Array[Array[Int]]): Unit = {
    _allExecutorServerPorts = ports
  }

  def getAllExecutorServerPorts: Array[Array[Int]] = _allExecutorServerPorts

  def setConfig(config: Config): Unit = {
    _config = config
  }

  def getConfig: Config = _config

  def getNumNodes: Int = _config.workers.length + 1
}
