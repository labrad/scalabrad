package org.labrad.data

import io.netty.buffer.{ByteBuf, Unpooled}
import java.io.{ByteArrayInputStream, InputStream, IOException}
import java.nio.ByteOrder
import java.nio.ByteOrder.BIG_ENDIAN
import java.nio.charset.StandardCharsets.UTF_8
import org.labrad.types.Type

case class Packet(id: Int, target: Long, context: Context, records: Seq[Record]) {
  def toBytes(implicit bo: ByteOrder = BIG_ENDIAN): Array[Byte] = {
    val buf = Unpooled.buffer().order(bo)
    writeTo(buf)
    buf.toByteArray
  }

  def writeTo(buf: ByteBuf): Unit = {
    buf.writeInt(context.high.toInt)
    buf.writeInt(context.low.toInt)
    buf.writeInt(id)
    buf.writeInt(target.toInt)
    buf.writeLen {
      for (record <- records) {
        record.writeTo(buf)
      }
    }
  }
}

object Packet {
  def forRequest(request: Request, requestNum: Int) = request match {
    case Request(target, context, records) => Packet(requestNum, target, context, records)
  }

  def forMessage(request: Request) = forRequest(request, 0)

  val HEADER = Type("(ww)iww")

  def fromBytes(bytes: Array[Byte])(implicit bo: ByteOrder = BIG_ENDIAN): Packet =
    fromBytes(new ByteArrayInputStream(bytes))

  def fromBytes(in: InputStream)(implicit bo: ByteOrder): Packet = {
    def readBytes(n: Int): Array[Byte] = {
      val buf = Array.ofDim[Byte](n)
      var tot = 0
      while (tot < n) {
        in.read(buf, tot, n - tot) match {
          case i if i < 0 => throw new IOException("read failed")
          case i => tot += i
        }
      }
      buf
    }

    val headerBytes = readBytes(20)
    val hdr = FlatData.fromBytes(HEADER, headerBytes)
    val ((high, low), req, src, len) = hdr.get[((Long, Long), Int, Long, Long)]

    val data = readBytes(len.toInt)
    val recordBuf = Unpooled.wrappedBuffer(data).order(bo)
    val records = extractRecords(recordBuf)
    Packet(req, src, Context(high, low), records)
  }

  def extractRecords(buf: ByteBuf): Seq[Record] = {
    val records = Seq.newBuilder[Record]
    while (buf.readableBytes > 0) {
      val id = buf.readUnsignedInt

      val tagLen = buf.readInt
      val tag = buf.toString(buf.readerIndex, tagLen, UTF_8)
      val t = Type(tag)
      buf.skipBytes(tagLen)

      val dataLen = buf.readInt
      val dataBytes = Array.ofDim[Byte](dataLen)
      buf.readBytes(dataBytes)

      val data = FlatData.fromBytes(t, dataBytes)(buf.order)

      records += Record(id, data)
    }
    records.result
  }
}

case class Record(id: Long, data: Data) {
  def toBytes(implicit order: ByteOrder): Array[Byte] = {
    val buf = Unpooled.buffer().order(order)
    writeTo(buf)
    buf.toByteArray
  }

  def writeTo(buf: ByteBuf): Unit = {
    buf.writeInt(id.toInt)
    buf.writeLen { buf.writeUtf8String(data.t.toString) }
    buf.writeLen { buf.writeData(data) }
  }
}

case class Request(server: Long, context: Context = Context(0, 0), records: Seq[Record] = Nil)

case class NameRecord(name: String, data: Data = Data.NONE)
case class NameRequest(server: String, context: Context = Context(0, 0), records: Seq[NameRecord] = Seq.empty)

case class Message(source: Long, context: Context, msg: Long, data: Data)

