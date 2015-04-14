package org.labrad

import io.netty.buffer.ByteBuf
import java.nio.charset.StandardCharsets.UTF_8

package object data {
  implicit class Flattenable[T](val value: T) extends AnyVal {
    /**
     * Flatten this scala object into labrad data.
     *
     * There must be a setter for this type T available
     * in implicit scope.
     */
    def toData(tag: String)(implicit setter: Setter[T]): Data = {
      val data = Data(tag)
      data.set(value)
      data
    }
  }

  implicit class RichByteBuf(val buf: ByteBuf) extends AnyVal {
    /**
     * Write the length of a chunk of data as a 4-byte prefix field.
     * We do this by first reserving so space for the length field
     * and recording its location, then executing the provided function
     * and keeping track of how many bytes are written, and finally
     * going back and filling in the correct length.
     */
    def writeLen(f: => Unit): ByteBuf = {
      val idx = buf.writerIndex
      buf.writeInt(0)
      val start = buf.writerIndex
      f
      val len = buf.writerIndex - start
      buf.setInt(idx, len)
    }

    def writeData(data: Data): ByteBuf = {
      data.flatten(buf)
      buf
    }

    def writeUtf8String(s: String): ByteBuf = {
      buf.writeBytes(s.getBytes(UTF_8))
    }

    /**
     * Consume all readable bytes of this byte buffer as a byte array.
     */
    def toByteArray: Array[Byte] = {
      val bytes = Array.ofDim[Byte](buf.readableBytes)
      buf.getBytes(buf.readerIndex, bytes)
      bytes
    }
  }
}
