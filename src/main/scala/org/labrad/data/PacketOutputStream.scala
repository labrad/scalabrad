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

package org.labrad.data

import scala.collection.JavaConversions._

import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.OutputStream

import org.labrad.types.Type

// Output stream that writes LabRAD packets.
class PacketOutputStream(out: OutputStream) extends BufferedOutputStream(out) {

  // Write a packet to the output stream.
  def writePacket(packet: Packet) {
    // flatten records
    val flatRecs = packet.records.flatMap {
      case Record(id, data) =>
        Cluster(Word(id), Str(data.tag), Bytes(data.toBytes)).toBytes
    }.toArray

    // flatten packet header and append records
    val ctx = packet.context
    val packetData = Cluster(Cluster(Word(ctx.high), Word(ctx.low)),
                             Integer(packet.id),
                             Word(packet.target),
                             Bytes(flatRecs))
    val bytes = packetData.toBytes
    out.write(bytes)
    out.flush
  }
}
