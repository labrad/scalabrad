package org.labrad

import io.netty.buffer.ByteBuf
import java.nio.ByteOrder
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
    def getIntOrdered(index: Int)(implicit bo: ByteOrder): Int = {
      bo match {
        case ByteOrder.BIG_ENDIAN => buf.getInt(index)
        case ByteOrder.LITTLE_ENDIAN => buf.getIntLE(index)
      }
    }

    def readIntOrdered()(implicit bo: ByteOrder): Int = {
      bo match {
        case ByteOrder.BIG_ENDIAN => buf.readInt()
        case ByteOrder.LITTLE_ENDIAN => buf.readIntLE()
      }
    }

    def readUnsignedIntOrdered()(implicit bo: ByteOrder): Long = {
      bo match {
        case ByteOrder.BIG_ENDIAN => buf.readUnsignedInt()
        case ByteOrder.LITTLE_ENDIAN => buf.readUnsignedIntLE()
      }
    }

    def setIntOrdered(index: Int, value: Int)(implicit bo: ByteOrder): ByteBuf = {
      bo match {
        case ByteOrder.BIG_ENDIAN => buf.setInt(index, value)
        case ByteOrder.LITTLE_ENDIAN => buf.setIntLE(index, value)
      }
    }

    def writeIntOrdered(value: Int)(implicit bo: ByteOrder): ByteBuf = {
      bo match {
        case ByteOrder.BIG_ENDIAN => buf.writeInt(value)
        case ByteOrder.LITTLE_ENDIAN => buf.writeIntLE(value)
      }
    }

    def writeLongOrdered(value: Long)(implicit bo: ByteOrder): ByteBuf = {
      bo match {
        case ByteOrder.BIG_ENDIAN => buf.writeLong(value)
        case ByteOrder.LITTLE_ENDIAN => buf.writeLongLE(value)
      }
    }

    def writeDoubleOrdered(value: Double)(implicit bo: ByteOrder): ByteBuf = {
      bo match {
        case ByteOrder.BIG_ENDIAN => buf.writeDouble(value)
        case ByteOrder.LITTLE_ENDIAN =>
          buf.writeLongLE(java.lang.Double.doubleToRawLongBits(value))
      }
    }

    /**
     * Write the length of a chunk of data as a 4-byte prefix field.
     * We do this by first reserving so space for the length field
     * and recording its location, then executing the provided function
     * and keeping track of how many bytes are written, and finally
     * going back and filling in the correct length.
     */
    def writeLen(f: => Unit)(implicit bo: ByteOrder): ByteBuf = {
      val idx = buf.writerIndex
      writeIntOrdered(0)
      val start = buf.writerIndex
      f
      val len = buf.writerIndex - start
      setIntOrdered(idx, len)
    }

    def writeData(data: Data)(implicit bo: ByteOrder): ByteBuf = {
      data.flatten(buf, bo)
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
