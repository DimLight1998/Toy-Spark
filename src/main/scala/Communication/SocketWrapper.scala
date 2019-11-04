package Communication

import java.net.Socket
import java.nio.ByteBuffer

object SocketWrapper {
  private val _sizeIndicatorNumByte = 4

  def fullyRead(socket: Socket, buffer: Array[Byte]): Unit = {
    var remainSize = buffer.length
    var offset     = 0
    while (remainSize > 0) {
      val readSize = socket.getInputStream.read(buffer, offset, remainSize)
      offset += readSize
      remainSize -= readSize
    }
  }

  def extractBinaryMessage(socket: Socket): Array[Byte] = {
    val header = Array.ofDim[Byte](_sizeIndicatorNumByte)
    fullyRead(socket, header)
    val messageLength = ByteBuffer.wrap(header).getInt
    val content       = Array.ofDim[Byte](messageLength)
    fullyRead(socket, content)
    content
  }

  def extractTextMessage(socket: Socket): String = {
    val content = extractBinaryMessage(socket)
    new String(content, "UTF-8")
  }

  def sendBinaryMessage(socket: Socket, message: Array[Byte]): Unit = {
    val size   = message.length
    val header = ByteBuffer.allocate(_sizeIndicatorNumByte).putInt(size).array()
    socket.getOutputStream.write(header)
    socket.getOutputStream.write(message)
  }

  def sendTextMessage(socket: Socket, message: String): Unit = {
    sendBinaryMessage(socket, message.getBytes("UTF-8"))
  }
}
