package org.labrad.data

import java.io.{ByteArrayInputStream, InputStream, IOException}
import java.nio.ByteOrder
import java.nio.ByteOrder.BIG_ENDIAN
import org.labrad.types.Type
import org.labrad.util.Logging

case class Packet(id: Int, target: Long, context: Context, records: Seq[Record]) {
  def toBytes(implicit bo: ByteOrder = BIG_ENDIAN) = {
    val flatRecs = records.flatMap(_.toBytes).toArray
    val packetData = Cluster(context.toData, Integer(id), UInt(target), Bytes(flatRecs))
    packetData.toBytes
  }
}

object Packet extends Logging {
  def forRequest(request: Request, requestNum: Int) = request match {
    case Request(target, context, records) => Packet(requestNum, target, context, records)
  }

  def forMessage(request: Request) = forRequest(request, 0)

  val HEADER = Type("(ww)iww")
  val RECORD = Type("wss")

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
    val hdr = Data.fromBytes(headerBytes, HEADER)
    val Cluster(Cluster(UInt(high), UInt(low)), Integer(req), UInt(src), UInt(len)) = hdr

    val buf = readBytes(len.toInt)
    val records = extractRecords(buf)
    Packet(req, src, Context(high, low), records)
  }

  def extractRecords(bytes: Array[Byte])(implicit bo: ByteOrder): Seq[Record] = {
    val stream = new ByteArrayInputStream(bytes)
    val records = Seq.newBuilder[Record]
    while (stream.available > 0) {
      val Cluster(UInt(id), Str(tag), Bytes(data)) = Data.fromBytes(stream, RECORD)
      records += Record(id, Data.fromBytes(data, Type(tag)))
    }
    records.result
  }
}

case class Record(id: Long, data: Data) {
  def toBytes(implicit order: ByteOrder) =
    Cluster(UInt(id), Str(data.t.toString), Bytes(data.toBytes)).toBytes
}
case class Request(server: Long, context: Context = Context(0, 0), records: Seq[Record] = Nil)

case class NameRecord(name: String, data: Data = Data.NONE)
case class NameRequest(server: String, context: Context = Context(0, 0), records: Seq[NameRecord] = Seq.empty)

case class Message(source: Long, context: Context, msg: Long, data: Data)

