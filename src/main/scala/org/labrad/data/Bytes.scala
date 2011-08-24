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


  // int (signed 32-bit integer)
  def getInt(b: ByteArrayView): Int = getInt(b.bytes, b.offset)
  def getInt(buf: Array[Byte], ofs: Int): Int =
    ((0xFF & buf(ofs + 0).toInt) << 24 |
     (0xFF & buf(ofs + 1).toInt) << 16 |
     (0xFF & buf(ofs + 2).toInt) << 8 |
     (0xFF & buf(ofs + 3).toInt) << 0).toInt

  def setInt(b: ByteArrayView, data: Int) {
    setInt(b.bytes, b.offset, data)
  }
  def setInt(buf: Array[Byte], ofs: Int, data: Int) {
    buf(ofs + 0) = ((data & 0xFF000000) >> 24).toByte
    buf(ofs + 1) = ((data & 0x00FF0000) >> 16).toByte
    buf(ofs + 2) = ((data & 0x0000FF00) >> 8).toByte
    buf(ofs + 3) = ((data & 0x000000FF) >> 0).toByte
  }

  /**
   * Read an int from a byte stream.
   * 
   * @param is
   * @return
   * @throws IOException
   */
  def readInt(is: ByteArrayInputStream): Int = {
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
  def writeInt(os: ByteArrayOutputStream, data: Int) {
    val bytes = Array.ofDim[Byte](4)
    setInt(bytes, 0, data)
    os.write(bytes)
  }


  // word (unsigned 32-bit integer)
  def getWord(b: ByteArrayView): Long = getWord(b.bytes, b.offset)
  def getWord(buf: Array[Byte], ofs: Int): Long =
    ((0xFF & buf(ofs + 0).toLong) << 24 |
     (0xFF & buf(ofs + 1).toLong) << 16 |
     (0xFF & buf(ofs + 2).toLong) << 8 |
     (0xFF & buf(ofs + 3).toLong) << 0).toLong

  def setWord(b: ByteArrayView, data: Long) {
    setWord(b.bytes, b.offset, data)
  }
  def setWord(buf: Array[Byte], ofs: Int, data: Long) {
    buf(ofs + 0) = ((data & 0xFF000000) >> 24).toByte
    buf(ofs + 1) = ((data & 0x00FF0000) >> 16).toByte
    buf(ofs + 2) = ((data & 0x0000FF00) >> 8).toByte
    buf(ofs + 3) = ((data & 0x000000FF) >> 0).toByte
  }


  // long (64-bit integer)
  def getLong(b: ByteArrayView): Long = getLong(b.bytes, b.offset)
  def getLong(buf: Array[Byte], ofs: Int) =
    ((0xFF & buf(ofs + 0).toLong) << 56 |
     (0xFF & buf(ofs + 1).toLong) << 48 |
     (0xFF & buf(ofs + 2).toLong) << 40 |
     (0xFF & buf(ofs + 3).toLong) << 32 |
     (0xFF & buf(ofs + 4).toLong) << 24 |
     (0xFF & buf(ofs + 5).toLong) << 16 |
     (0xFF & buf(ofs + 6).toLong) << 8 |
     (0xFF & buf(ofs + 7).toLong) << 0).toLong

  def setLong(b: ByteArrayView, data: Long) {
    setLong(b.bytes, b.offset, data)
  }
  def setLong(buf: Array[Byte], ofs: Int, data: Long) {
    buf(ofs + 0) = ((data & 0xFF00000000000000L) >> 56).toByte
    buf(ofs + 1) = ((data & 0x00FF000000000000L) >> 48).toByte
    buf(ofs + 2) = ((data & 0x0000FF0000000000L) >> 40).toByte
    buf(ofs + 3) = ((data & 0x000000FF00000000L) >> 32).toByte
    buf(ofs + 4) = ((data & 0x00000000FF000000L) >> 24).toByte
    buf(ofs + 5) = ((data & 0x0000000000FF0000L) >> 16).toByte
    buf(ofs + 6) = ((data & 0x000000000000FF00L) >> 8).toByte
    buf(ofs + 7) = ((data & 0x00000000000000FFL) >> 0).toByte
  }

  
  // doubles
  def getDouble(b: ByteArrayView): Double = getDouble(b.bytes, b.offset)
  def getDouble(buf: Array[Byte], ofs: Int) =
    java.lang.Double.longBitsToDouble(getLong(buf, ofs))

  def setDouble(b: ByteArrayView, data: Double) {
    setDouble(b.bytes, b.offset, data)
  }
  def setDouble(buf: Array[Byte], ofs: Int, data: Double) {
    setLong(buf, ofs, java.lang.Double.doubleToRawLongBits(data))
  }


  // complex numbers
  def getComplex(b: ByteArrayView): Complex = getComplex(b.bytes, b.offset)
  def getComplex(buf: Array[Byte], ofs: Int) =
    new Complex(getDouble(buf, ofs), getDouble(buf, ofs + 8))

  def setComplex(b: ByteArrayView, data: Complex) {
    setComplex(b.bytes, b.offset, data)
  }
  def setComplex(buf: Array[Byte], ofs: Int, data: Complex) {
    setDouble(buf, ofs, data.real)
    setDouble(buf, ofs + 8, data.imag)
  }


  // basic tests
  def main(args: Array[String]) {
    
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
