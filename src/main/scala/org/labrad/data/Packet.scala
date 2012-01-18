/*
 * Copyright 2008 Matthew Neeley
 * 
 * This file is part of JLabrad.
 *
 * JLabrad is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 * 
 * JLabrad is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with JLabrad.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.labrad
package data

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.IOException
import java.nio.ByteOrder

import Constants.DEFAULT_CONTEXT
import types.Type

case class Packet(id: Int, target: Long, context: Context, records: Seq[Record]) {
  def toBytes(implicit order: ByteOrder = ByteOrder.BIG_ENDIAN) = {
    val flatRecs = records.flatMap(_.toBytes).toArray
    val packetData = Cluster(context.toData, Integer(id), Word(target), Bytes(flatRecs))
    packetData.toBytes
  }
}

object Packet {
  def forRequest(request: Request, requestNum: Int) = request match {
    case Request(target, context, records) => Packet(requestNum, target, context, records)
  }

  def forMessage(request: Request) = forRequest(request, 0)
  
  def fromBytes(bytes: Array[Byte])
               (implicit order: ByteOrder = ByteOrder.BIG_ENDIAN): Packet =
    fromBytes(new ByteArrayInputStream(bytes))
  
  def fromBytes(in: InputStream)
               (implicit order: ByteOrder): Packet = {
    val headerBytes = readBytes(in, 20)
    val hdr = Data.fromBytes(headerBytes, Type.HEADER)
    val Cluster(Cluster(Word(high), Word(low)), Integer(req), Word(src), Word(len)) = hdr

    val buf = readBytes(in, len.toInt)
    val records = extractRecords(buf)
    Packet(req, src, Context(high, low), records)
  }
  
  private def extractRecords(bytes: Array[Byte])
                            (implicit order: ByteOrder) = {
    val stream = new ByteArrayInputStream(bytes)
    val records = Seq.newBuilder[Record]
    while (stream.available > 0) {
      val recdata = Data.fromBytes(stream, Type.RECORD)
      val Cluster(Word(id), Str(tag), Bytes(data)) = recdata
      records += Record(id, Data.fromBytes(data, Type(tag)))
    }
    records.result
  }
  
  // Read the specified number of bytes from the input stream.
  private def readBytes(in: InputStream, n: Int): Array[Byte] = {
    val data = Array.ofDim[Byte](n)
    var soFar = 0

    while (soFar < n) {
      val read = in.read(data, soFar, n - soFar)
      if (read < 0) throw new IOException("Socket closed.")
      soFar += read
    }
    data
  }
}

case class Record(id: Long, data: Data = Data.EMPTY) {
  def toBytes(implicit order: ByteOrder) =
    Cluster(Word(id), Str(data.tag), Bytes(data.toBytes)).toBytes
}
case class Request(server: Long, context: Context = DEFAULT_CONTEXT, records: Seq[Record] = Seq.empty)

case class NameRecord(name: String, data: Data = Data.EMPTY)
case class NameRequest(server: String, context: Context = DEFAULT_CONTEXT, records: Seq[NameRecord] = Seq.empty)

case class Message(source: Long, context: Context, msg: Long, data: Data)

