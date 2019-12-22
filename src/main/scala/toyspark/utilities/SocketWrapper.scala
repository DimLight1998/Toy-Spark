package toyspark.utilities

import java.net.{InetSocketAddress, Socket}
import java.nio.ByteBuffer

object SocketWrapper {
  implicit class SocketWrapperK(val socket: Socket) {
    def connectChaining(addr: InetSocketAddress): Socket = {
      socket.connect(addr)
      socket
    }
    def recvBinaryMessage(): Array[Byte] = SocketWrapper.recvBinaryMessage(socket)
    def recvTextMessage(): String        = SocketWrapper.recvTextMessage(socket)
    def recvBinaryMessageThenClose(): Array[Byte] = {
      val ret = recvBinaryMessage()
      socket.close()
      ret
    }
    def recvTextMessageThenClose(): String = {
      val ret = recvTextMessage()
      socket.close()
      ret
    }
    def sendBinaryMessage(message: Array[Byte]): Socket = {
      SocketWrapper.sendBinaryMessage(socket, message)
      socket
    }
    def sendTextMessage(message: String): Socket = {
      SocketWrapper.sendTextMessage(socket, message)
      socket
    }
  }

  private val _sizeIndicatorNumByte = 4

  private def fullyRead(socket: Socket, buffer: Array[Byte]): Unit = {
    var remainSize = buffer.length
    var offset     = 0
    while (remainSize > 0) {
      val readSize = socket.getInputStream.read(buffer, offset, remainSize)
      offset += readSize
      remainSize -= readSize
    }
  }

  private def recvBinaryMessage(socket: Socket): Array[Byte] = {
    val header = Array.ofDim[Byte](_sizeIndicatorNumByte)
    fullyRead(socket, header)
    val messageLength = ByteBuffer.wrap(header).getInt
    val content       = Array.ofDim[Byte](messageLength)
    fullyRead(socket, content)
    content
  }

  private def recvTextMessage(socket: Socket): String = {
    val content = recvBinaryMessage(socket)
    new String(content, "UTF-8")
  }

  private def sendBinaryMessage(socket: Socket, message: Array[Byte]): Unit = {
    val size   = message.length
    val header = ByteBuffer.allocate(_sizeIndicatorNumByte).putInt(size).array()
    socket.getOutputStream.write(header)
    socket.getOutputStream.write(message)
  }

  private def sendTextMessage(socket: Socket, message: String): Unit = {
    sendBinaryMessage(socket, message.getBytes("UTF-8"))
  }
}
