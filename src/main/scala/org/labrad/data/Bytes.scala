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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteOrder
import java.util.Random

import scala.math

/**
 * Functions for storing basic data types in arrays of bytes, and
 * for converting arrays of bytes back into basic data types.
 */
object ByteManip {

  // boolean (true/false value)
  def getBool(b: ByteArrayView): Boolean = getBool(b.bytes, b.offset)
  def getBool(buf: Array[Byte], ofs: Int): Boolean = buf(ofs) != 0

  def setBool(b: ByteArrayView, data: Boolean) {
    setBool(b.bytes, b.offset, data)
  }
  def setBool(buf: Array[Byte], ofs: Int, data: Boolean) {
    buf(ofs) = (if (data) 1 else 0)
  }
  
  def readBool(is: ByteArrayInputStream)(implicit order: ByteOrder): Boolean = {
    val bytes = Array.ofDim[Byte](1)
    is.read(bytes, 0, 1)
    getBool(bytes, 0)
  }

  def writeBool(os: ByteArrayOutputStream, data: Boolean)(implicit order: ByteOrder) {
    val bytes = Array.ofDim[Byte](1)
    setBool(bytes, 0, data)
    os.write(bytes)
  }


  // int (signed 32-bit integer)
  def getInt(b: ByteArrayView)(implicit order: ByteOrder): Int = getInt(b.bytes, b.offset)
  def getInt(buf: Array[Byte], ofs: Int)(implicit order: ByteOrder): Int = order match {
    case ByteOrder.BIG_ENDIAN => getIntBE(buf, ofs)
    case ByteOrder.LITTLE_ENDIAN => getIntLE(buf, ofs)
  }
  def getIntBE(buf: Array[Byte], ofs: Int): Int =
    ((0xFF & buf(ofs + 0).toInt) << 24 |
     (0xFF & buf(ofs + 1).toInt) << 16 |
     (0xFF & buf(ofs + 2).toInt) << 8 |
     (0xFF & buf(ofs + 3).toInt) << 0).toInt
   def getIntLE(buf: Array[Byte], ofs: Int): Int =
    ((0xFF & buf(ofs + 3).toInt) << 24 |
     (0xFF & buf(ofs + 2).toInt) << 16 |
     (0xFF & buf(ofs + 1).toInt) << 8 |
     (0xFF & buf(ofs + 0).toInt) << 0).toInt

  def setInt(b: ByteArrayView, data: Int)(implicit order: ByteOrder) {
    setInt(b.bytes, b.offset, data)
  }
  def setInt(buf: Array[Byte], ofs: Int, data: Int)(implicit order: ByteOrder) {
    order match {
      case ByteOrder.BIG_ENDIAN => setIntBE(buf, ofs, data)
      case ByteOrder.LITTLE_ENDIAN => setIntLE(buf, ofs, data)
    }
  }
  def setIntBE(buf: Array[Byte], ofs: Int, data: Int) {
    buf(ofs + 0) = ((data & 0xFF000000) >> 24).toByte
    buf(ofs + 1) = ((data & 0x00FF0000) >> 16).toByte
    buf(ofs + 2) = ((data & 0x0000FF00) >> 8).toByte
    buf(ofs + 3) = ((data & 0x000000FF) >> 0).toByte
  }
  def setIntLE(buf: Array[Byte], ofs: Int, data: Int) {
    buf(ofs + 3) = ((data & 0xFF000000) >> 24).toByte
    buf(ofs + 2) = ((data & 0x00FF0000) >> 16).toByte
    buf(ofs + 1) = ((data & 0x0000FF00) >> 8).toByte
    buf(ofs + 0) = ((data & 0x000000FF) >> 0).toByte
  }

  /**
   * Read an int from a byte stream.
   * 
   * @param is
   * @return
   * @throws IOException
   */
  def readInt(is: ByteArrayInputStream)(implicit order: ByteOrder): Int = {
    val bytes = Array.ofDim[Byte](4)
    is.read(bytes, 0, 4)
    getInt(bytes, 0)
  }

  /**
   * Write an int to a byte stream.
   * 
   * @param os
   * @param data
   * @throws IOException
   */
  def writeInt(os: ByteArrayOutputStream, data: Int)(implicit order: ByteOrder) {
    val bytes = Array.ofDim[Byte](4)
    setInt(bytes, 0, data)
    os.write(bytes)
  }


  // word (unsigned 32-bit integer)
  def getWord(b: ByteArrayView)(implicit order: ByteOrder): Long = getWord(b.bytes, b.offset)
  def getWord(buf: Array[Byte], ofs: Int)(implicit order: ByteOrder): Long = order match {
    case ByteOrder.BIG_ENDIAN => getWordBE(buf, ofs)
    case ByteOrder.LITTLE_ENDIAN => getWordLE(buf, ofs)
  }
  def getWordBE(buf: Array[Byte], ofs: Int): Long =
    ((0xFF & buf(ofs + 0).toLong) << 24 |
     (0xFF & buf(ofs + 1).toLong) << 16 |
     (0xFF & buf(ofs + 2).toLong) << 8 |
     (0xFF & buf(ofs + 3).toLong) << 0).toLong
  def getWordLE(buf: Array[Byte], ofs: Int): Long =
    ((0xFF & buf(ofs + 3).toLong) << 24 |
     (0xFF & buf(ofs + 2).toLong) << 16 |
     (0xFF & buf(ofs + 1).toLong) << 8 |
     (0xFF & buf(ofs + 0).toLong) << 0).toLong

  def setWord(b: ByteArrayView, data: Long)(implicit order: ByteOrder) {
    setWord(b.bytes, b.offset, data)
  }
  def setWord(buf: Array[Byte], ofs: Int, data: Long)(implicit order: ByteOrder) {
    order match {
      case ByteOrder.BIG_ENDIAN => setWordBE(buf, ofs, data)
      case ByteOrder.LITTLE_ENDIAN => setWordLE(buf, ofs, data)
    }
  }
  def setWordBE(buf: Array[Byte], ofs: Int, data: Long) {
    buf(ofs + 0) = ((data & 0xFF000000) >> 24).toByte
    buf(ofs + 1) = ((data & 0x00FF0000) >> 16).toByte
    buf(ofs + 2) = ((data & 0x0000FF00) >> 8).toByte
    buf(ofs + 3) = ((data & 0x000000FF) >> 0).toByte
  }
  def setWordLE(buf: Array[Byte], ofs: Int, data: Long) {
    buf(ofs + 3) = ((data & 0xFF000000) >> 24).toByte
    buf(ofs + 2) = ((data & 0x00FF0000) >> 16).toByte
    buf(ofs + 1) = ((data & 0x0000FF00) >> 8).toByte
    buf(ofs + 0) = ((data & 0x000000FF) >> 0).toByte
  }
  
  def readWord(is: ByteArrayInputStream)(implicit order: ByteOrder): Long = {
    val bytes = Array.ofDim[Byte](4)
    is.read(bytes, 0, 4)
    getWord(bytes, 0)
  }

  def writeWord(os: ByteArrayOutputStream, data: Long)(implicit order: ByteOrder) {
    val bytes = Array.ofDim[Byte](4)
    setWord(bytes, 0, data)
    os.write(bytes)
  }



  // long (64-bit integer)
  def getLong(b: ByteArrayView)(implicit order: ByteOrder): Long = getLong(b.bytes, b.offset)
  def getLong(buf: Array[Byte], ofs: Int)(implicit order: ByteOrder): Long = order match {
    case ByteOrder.BIG_ENDIAN => getLongBE(buf, ofs)
    case ByteOrder.LITTLE_ENDIAN => getLongLE(buf, ofs)
  }
  def getLongBE(buf: Array[Byte], ofs: Int): Long =
    ((0xFF & buf(ofs + 0).toLong) << 56 |
     (0xFF & buf(ofs + 1).toLong) << 48 |
     (0xFF & buf(ofs + 2).toLong) << 40 |
     (0xFF & buf(ofs + 3).toLong) << 32 |
     (0xFF & buf(ofs + 4).toLong) << 24 |
     (0xFF & buf(ofs + 5).toLong) << 16 |
     (0xFF & buf(ofs + 6).toLong) << 8 |
     (0xFF & buf(ofs + 7).toLong) << 0).toLong
  def getLongLE(buf: Array[Byte], ofs: Int): Long =
    ((0xFF & buf(ofs + 7).toLong) << 56 |
     (0xFF & buf(ofs + 6).toLong) << 48 |
     (0xFF & buf(ofs + 5).toLong) << 40 |
     (0xFF & buf(ofs + 4).toLong) << 32 |
     (0xFF & buf(ofs + 3).toLong) << 24 |
     (0xFF & buf(ofs + 2).toLong) << 16 |
     (0xFF & buf(ofs + 1).toLong) << 8 |
     (0xFF & buf(ofs + 0).toLong) << 0).toLong

  def setLong(b: ByteArrayView, data: Long)(implicit order: ByteOrder) {
    setLong(b.bytes, b.offset, data)
  }
  def setLong(buf: Array[Byte], ofs: Int, data: Long)(implicit order: ByteOrder) {
    order match {
      case ByteOrder.BIG_ENDIAN => setLongBE(buf, ofs, data)
      case ByteOrder.LITTLE_ENDIAN => setLongLE(buf, ofs, data)
    }
  }
  def setLongBE(buf: Array[Byte], ofs: Int, data: Long) {
    buf(ofs + 0) = ((data & 0xFF00000000000000L) >> 56).toByte
    buf(ofs + 1) = ((data & 0x00FF000000000000L) >> 48).toByte
    buf(ofs + 2) = ((data & 0x0000FF0000000000L) >> 40).toByte
    buf(ofs + 3) = ((data & 0x000000FF00000000L) >> 32).toByte
    buf(ofs + 4) = ((data & 0x00000000FF000000L) >> 24).toByte
    buf(ofs + 5) = ((data & 0x0000000000FF0000L) >> 16).toByte
    buf(ofs + 6) = ((data & 0x000000000000FF00L) >> 8).toByte
    buf(ofs + 7) = ((data & 0x00000000000000FFL) >> 0).toByte
  }
  def setLongLE(buf: Array[Byte], ofs: Int, data: Long) {
    buf(ofs + 7) = ((data & 0xFF00000000000000L) >> 56).toByte
    buf(ofs + 6) = ((data & 0x00FF000000000000L) >> 48).toByte
    buf(ofs + 5) = ((data & 0x0000FF0000000000L) >> 40).toByte
    buf(ofs + 4) = ((data & 0x000000FF00000000L) >> 32).toByte
    buf(ofs + 3) = ((data & 0x00000000FF000000L) >> 24).toByte
    buf(ofs + 2) = ((data & 0x0000000000FF0000L) >> 16).toByte
    buf(ofs + 1) = ((data & 0x000000000000FF00L) >> 8).toByte
    buf(ofs + 0) = ((data & 0x00000000000000FFL) >> 0).toByte
  }
  
  def readLong(is: ByteArrayInputStream)(implicit order: ByteOrder): Long = {
    val bytes = Array.ofDim[Byte](8)
    is.read(bytes, 0, 8)
    getLong(bytes, 0)
  }

  def writeLong(os: ByteArrayOutputStream, data: Long)(implicit order: ByteOrder) {
    val bytes = Array.ofDim[Byte](8)
    setLong(bytes, 0, data)
    os.write(bytes)
  }

  
  // doubles
  def getDouble(b: ByteArrayView)(implicit order: ByteOrder): Double = getDouble(b.bytes, b.offset)
  def getDouble(buf: Array[Byte], ofs: Int)(implicit order: ByteOrder) =
    java.lang.Double.longBitsToDouble(getLong(buf, ofs))

  def setDouble(b: ByteArrayView, data: Double)(implicit order: ByteOrder) {
    setDouble(b.bytes, b.offset, data)
  }
  def setDouble(buf: Array[Byte], ofs: Int, data: Double)(implicit order: ByteOrder) {
    setLong(buf, ofs, java.lang.Double.doubleToRawLongBits(data))
  }
  
  def readDouble(is: ByteArrayInputStream)(implicit order: ByteOrder): Double = {
    java.lang.Double.longBitsToDouble(readLong(is))
  }

  def writeDouble(os: ByteArrayOutputStream, data: Double)(implicit order: ByteOrder) {
    writeLong(os, java.lang.Double.doubleToRawLongBits(data))
  }


  // complex numbers
  def getComplex(b: ByteArrayView)(implicit order: ByteOrder): Complex = getComplex(b.bytes, b.offset)
  def getComplex(buf: Array[Byte], ofs: Int)(implicit order: ByteOrder) =
    new Complex(getDouble(buf, ofs), getDouble(buf, ofs + 8))

  def setComplex(b: ByteArrayView, data: Complex)(implicit order: ByteOrder) {
    setComplex(b.bytes, b.offset, data)
  }
  def setComplex(buf: Array[Byte], ofs: Int, data: Complex)(implicit order: ByteOrder) {
    setDouble(buf, ofs, data.real)
    setDouble(buf, ofs + 8, data.imag)
  }

  def readComplex(is: ByteArrayInputStream)(implicit order: ByteOrder): Complex = {
    new Complex(readDouble(is), readDouble(is))
  }

  def writeComplex(os: ByteArrayOutputStream, data: Complex)(implicit order: ByteOrder) {
    writeDouble(os, data.real)
    writeDouble(os, data.imag)
  }

  // basic tests
  def main(args: Array[String]) {
    for (byteOrder <- List(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)) {
      implicit val order = byteOrder
      
      val bs = Array.ofDim[Byte](100)
      val rand = new Random
  
      for (count <- 0 until 1000) {
        val b = rand.nextBoolean
        setBool(bs, 0, b)
        assert(b == getBool(bs, 0))
      }
      println("Bool okay.")
  
      for (count <- 0 until 1000000) {
        val i = rand.nextInt
        setInt(bs, 0, i)
        assert(i == getInt(bs, 0))
      }
      println("Int okay.")
  
      for (count <- 0 until 1000000) {
        val l = math.abs(rand.nextLong) % 4294967296L
        setWord(bs, 0, l)
        assert(l == getWord(bs, 0))
      }
      println("Word okay.")
  
      for (count <- 0 until 1000000) {
        val l = rand.nextLong
        setLong(bs, 0, l)
        assert(l == getLong(bs, 0))
      }
      println("Long okay.")
  
      for (count <- 0 until 100000) {
        val d = rand.nextGaussian
        setDouble(bs, 0, d)
        assert(d == getDouble(bs, 0))
      }
      println("Double okay.")
  
      for (count <- 0 until 100000) {
        val re = rand.nextGaussian
        val im = rand.nextGaussian
        val c1 = new Complex(re, im)
        setComplex(bs, 0, c1)
        val c2 = getComplex(bs, 0)
        assert(c1.real == c2.real)
        assert(c1.imag == c2.imag)
        assert(c1 == c2)
      }
      println("Complex okay.")
    }
  }
}

class ByteManip

