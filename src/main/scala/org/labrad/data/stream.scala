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

import java.io.BufferedOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.FilterInputStream
import java.io.InputStream
import java.io.IOException
import java.io.OutputStream
import java.nio.ByteOrder

import scala.collection.mutable.ArrayBuffer

import types.Type

class PacketInputStream(in: InputStream)
                       (implicit order: ByteOrder = ByteOrder.BIG_ENDIAN)
  extends FilterInputStream(in) {

  // Read a single packet from the input stream.
  def readPacket = Packet.fromBytes(in)
}

// Output stream that writes LabRAD packets.
class PacketOutputStream(out: OutputStream)
                        (implicit order: ByteOrder = ByteOrder.BIG_ENDIAN)
  extends BufferedOutputStream(out) {

  // Write a packet to the output stream.
  def writePacket(packet: Packet) {
    out.write(packet.toBytes)
    out.flush
  }
}
