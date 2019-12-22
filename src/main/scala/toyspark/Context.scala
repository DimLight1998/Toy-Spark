package toyspark

import scala.collection.parallel.mutable.ParHashMap
import scala.collection.mutable.{HashMap => MutHashMap}
import java.net._

import toyspark.utilities.Config

import scala.collection.parallel.ParIterable

object Context {
  private var _nodeId: Int                                    = _ // ID for this node, start from 0 (used by master)
  private var _ctrlServerSocket: ServerSocket                 = _ // only available for master, handles control request
  private var _dataServerSocket: ServerSocket                 = _ // for all nodes, handles data request
  private var _dataServerContacts: Array[InetSocketAddress]   = _ // for all nodes, be used to send data request
  private var _config: Config                                 = _ // config file
  private val _datasetIDMap: MutHashMap[Dataset[_], Int]      = MutHashMap()
  private var _nextDatasetID: Int                             = 0
  private val _sendingBuffer: ParHashMap[(Int, Int), List[_]] = ParHashMap()

  def setNodeId(nodeId: Int): Unit = { _nodeId = nodeId }
  def getNodeId: Int               = _nodeId

  def isMaster: Boolean = _nodeId == 0

  def setCtrlServerSocket(serverSocket: ServerSocket): Unit = { _ctrlServerSocket = serverSocket }
  def getCtrlServerSocket: ServerSocket                     = _ctrlServerSocket

  def setDataServerSocket(serverSocket: ServerSocket): Unit = { _dataServerSocket = serverSocket }
  def getDataServerSocket: ServerSocket                     = _dataServerSocket
  def getDataServerSocketPort: Int                          = _dataServerSocket.getLocalPort

  def setDataServerContacts(contacts: Array[InetSocketAddress]): Unit = { _dataServerContacts = contacts }
  def getDataServerContacts: Array[InetSocketAddress]                 = _dataServerContacts
  def getWorkerDataServerContacts: Array[InetSocketAddress]           = _dataServerContacts.tail

  def setConfig(config: Config): Unit = { _config = config }
  def getConfig: Config               = _config
  def getNumNodes: Int                = _config.workers.length + 1

  def getOrAssignDatasetID(dataset: Dataset[_]): Int = {
    if (!_datasetIDMap.contains(dataset)) {
      _datasetIDMap(dataset) = _nextDatasetID
      _nextDatasetID += 1
    }
    _datasetIDMap(dataset)
  }

  def setSendingBufferEntry(datasetID: Int, partitionID: Int, data: List[_]): Unit = {
    _sendingBuffer((datasetID, partitionID)) = data
  }
  def getSendingBufferEntry(datasetID: Int, partitionID: Int): List[_] = _sendingBuffer((datasetID, partitionID))
  def getSendingBufferDataByDatasetID(datasetID: Int): List[List[_]] = {
    val filteredKeys = _sendingBuffer.keys.filter({ case (entryDsID, _) => entryDsID == datasetID }).toList
    filteredKeys.map(key => _sendingBuffer(key))
  }
  def removeSendingBufferEntryByDatasetID(datasetID: Int): Unit = {
    ???
  }
}
