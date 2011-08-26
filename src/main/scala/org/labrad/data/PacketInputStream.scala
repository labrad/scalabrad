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

import java.io.{ByteArrayInputStream, FilterInputStream, IOException, InputStream}
import java.nio.ByteOrder

import scala.collection.mutable.ArrayBuffer

import types.Type

class PacketInputStream(in: InputStream)(implicit order: ByteOrder = ByteOrder.BIG_ENDIAN) extends FilterInputStream(in) {

  // Read a single packet from the input stream.
  def readPacket = {
    val headerBytes = readBytes(20)
    val hdr = Data.fromBytes(headerBytes, Type.HEADER)
    val Cluster(Cluster(Word(high), Word(low)), Integer(req), Word(src), Word(len)) = hdr

    val buf = readBytes(len.toInt)
    val stream = new ByteArrayInputStream(buf)
    
    val records = Seq.newBuilder[Record]
    while (stream.available > 0) {
      val recdata = Data.fromBytes(stream, Type.RECORD)
      val Cluster(Word(id), Str(tag), Bytes(data)) = recdata
      records += Record(id, Data.fromBytes(data, Type(tag)))
    }
    Packet(req, src, Context(high, low), records.result)
  }

  // Read the specified number of bytes from the input stream.
  private def readBytes(n: Int) = {
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
