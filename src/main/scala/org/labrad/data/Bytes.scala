package org.labrad.data

import java.io.{InputStream, IOException, OutputStream}
import java.nio.ByteOrder
import scala.math

object EndianAwareByteArray {
  implicit class RichByteArray(val buf: Array[Byte]) extends AnyVal {
    def getBool(ofs: Int): Boolean = ByteManip.getBool(buf, ofs)
    def getInt(ofs: Int)(implicit bo: ByteOrder): Int = ByteManip.getInt(buf, ofs)
    def getUInt(ofs: Int)(implicit bo: ByteOrder): Long = ByteManip.getUInt(buf, ofs)
    def getLong(ofs: Int)(implicit bo: ByteOrder): Long = ByteManip.getLong(buf, ofs)
    def getDouble(ofs: Int)(implicit bo: ByteOrder): Double = ByteManip.getDouble(buf, ofs)

    def setBool(ofs: Int, data: Boolean) = ByteManip.setBool(buf, ofs, data)
    def setInt(ofs: Int, data: Int)(implicit bo: ByteOrder) = ByteManip.setInt(buf, ofs, data)
    def setUInt(ofs: Int, data: Long)(implicit bo: ByteOrder) = ByteManip.setUInt(buf, ofs, data)
    def setLong(ofs: Int, data: Long)(implicit bo: ByteOrder) = ByteManip.setLong(buf, ofs, data)
    def setDouble(ofs: Int, data: Double)(implicit bo: ByteOrder) = ByteManip.setDouble(buf, ofs, data)
  }
}

/** Functions for converting basic data types to/from arrays of bytes */
object ByteManip {

  // boolean (true/false value)
  def getBool(buf: Array[Byte], ofs: Int): Boolean = buf(ofs) != 0

  def setBool(buf: Array[Byte], ofs: Int, data: Boolean): Unit = {
    buf(ofs) = (if (data) 1 else 0)
  }

  def readBool(is: InputStream)(implicit order: ByteOrder): Boolean = {
    val bytes = Array.ofDim[Byte](1)
    is.read(bytes, 0, 1)
    getBool(bytes, 0)
  }

  def writeBool(os: OutputStream, data: Boolean)(implicit order: ByteOrder): Unit = {
    val bytes = Array.ofDim[Byte](1)
    setBool(bytes, 0, data)
    os.write(bytes)
  }


  // int (signed 32-bit integer)
  def getInt(buf: Array[Byte], ofs: Int)(implicit order: ByteOrder): Int =
    if (order == ByteOrder.BIG_ENDIAN) getIntBE(buf, ofs) else getIntLE(buf, ofs)
  private def getIntBE(buf: Array[Byte], ofs: Int): Int =
    ((0xFF & buf(ofs + 0).toInt) << 24 |
     (0xFF & buf(ofs + 1).toInt) << 16 |
     (0xFF & buf(ofs + 2).toInt) << 8 |
     (0xFF & buf(ofs + 3).toInt) << 0).toInt
  private def getIntLE(buf: Array[Byte], ofs: Int): Int =
    ((0xFF & buf(ofs + 0).toInt) << 0 |
     (0xFF & buf(ofs + 1).toInt) << 8 |
     (0xFF & buf(ofs + 2).toInt) << 16 |
     (0xFF & buf(ofs + 3).toInt) << 24).toInt

  def setInt(buf: Array[Byte], ofs: Int, data: Int)(implicit order: ByteOrder) =
    if (order == ByteOrder.BIG_ENDIAN) setIntBE(buf, ofs, data) else setIntLE(buf, ofs, data)
  private def setIntBE(buf: Array[Byte], ofs: Int, data: Int): Unit = {
    buf(ofs + 0) = ((data & 0xFF000000) >> 24).toByte
    buf(ofs + 1) = ((data & 0x00FF0000) >> 16).toByte
    buf(ofs + 2) = ((data & 0x0000FF00) >> 8).toByte
    buf(ofs + 3) = ((data & 0x000000FF) >> 0).toByte
  }
  private def setIntLE(buf: Array[Byte], ofs: Int, data: Int): Unit = {
    buf(ofs + 0) = ((data & 0x000000FF) >> 0).toByte
    buf(ofs + 1) = ((data & 0x0000FF00) >> 8).toByte
    buf(ofs + 2) = ((data & 0x00FF0000) >> 16).toByte
    buf(ofs + 3) = ((data & 0xFF000000) >> 24).toByte
  }

  /** Read an int from a byte stream. */
  def readInt(is: InputStream)(implicit order: ByteOrder): Int = {
    val bytes = Array.ofDim[Byte](4)
    is.read(bytes, 0, 4)
    getInt(bytes, 0)
  }

  /** Write an int to a byte stream. */
  def writeInt(os: OutputStream, data: Int)(implicit order: ByteOrder): Unit = {
    val bytes = Array.ofDim[Byte](4)
    setInt(bytes, 0, data)
    os.write(bytes)
  }


  // word (unsigned 32-bit integer)
  def getUInt(buf: Array[Byte], ofs: Int)(implicit order: ByteOrder): Long =
    if (order == ByteOrder.BIG_ENDIAN) getUIntBE(buf, ofs) else getUIntLE(buf, ofs)
  private def getUIntBE(buf: Array[Byte], ofs: Int): Long =
    ((0xFF & buf(ofs + 0).toLong) << 24 |
     (0xFF & buf(ofs + 1).toLong) << 16 |
     (0xFF & buf(ofs + 2).toLong) << 8 |
     (0xFF & buf(ofs + 3).toLong) << 0).toLong
  private def getUIntLE(buf: Array[Byte], ofs: Int): Long =
    ((0xFF & buf(ofs + 3).toLong) << 24 |
     (0xFF & buf(ofs + 2).toLong) << 16 |
     (0xFF & buf(ofs + 1).toLong) << 8 |
     (0xFF & buf(ofs + 0).toLong) << 0).toLong

  def setUInt(buf: Array[Byte], ofs: Int, data: Long)(implicit order: ByteOrder) =
    if (order == ByteOrder.BIG_ENDIAN) setUIntBE(buf, ofs, data) else setUIntLE(buf, ofs, data)
  private def setUIntBE(buf: Array[Byte], ofs: Int, data: Long): Unit = {
    buf(ofs + 0) = ((data & 0xFF000000) >> 24).toByte
    buf(ofs + 1) = ((data & 0x00FF0000) >> 16).toByte
    buf(ofs + 2) = ((data & 0x0000FF00) >> 8).toByte
    buf(ofs + 3) = ((data & 0x000000FF) >> 0).toByte
  }
  private def setUIntLE(buf: Array[Byte], ofs: Int, data: Long): Unit = {
    buf(ofs + 0) = ((data & 0x000000FF) >> 0).toByte
    buf(ofs + 1) = ((data & 0x0000FF00) >> 8).toByte
    buf(ofs + 2) = ((data & 0x00FF0000) >> 16).toByte
    buf(ofs + 3) = ((data & 0xFF000000) >> 24).toByte
  }

  def readUInt(is: InputStream)(implicit order: ByteOrder): Long = {
    val bytes = Array.ofDim[Byte](4)
    is.read(bytes, 0, 4)
    getUInt(bytes, 0)
  }

  def writeUInt(os: OutputStream, data: Long)(implicit order: ByteOrder): Unit = {
    val bytes = Array.ofDim[Byte](4)
    setUInt(bytes, 0, data)
    os.write(bytes)
  }



  // long (64-bit integer)
  def getLong(buf: Array[Byte], ofs: Int)(implicit order: ByteOrder): Long =
    if (order == ByteOrder.BIG_ENDIAN) getLongBE(buf, ofs) else getLongLE(buf, ofs)
  private def getLongBE(buf: Array[Byte], ofs: Int): Long =
    ((0xFF & buf(ofs + 0).toLong) << 56 |
     (0xFF & buf(ofs + 1).toLong) << 48 |
     (0xFF & buf(ofs + 2).toLong) << 40 |
     (0xFF & buf(ofs + 3).toLong) << 32 |
     (0xFF & buf(ofs + 4).toLong) << 24 |
     (0xFF & buf(ofs + 5).toLong) << 16 |
     (0xFF & buf(ofs + 6).toLong) << 8 |
     (0xFF & buf(ofs + 7).toLong) << 0).toLong
  private def getLongLE(buf: Array[Byte], ofs: Int): Long =
    ((0xFF & buf(ofs + 0).toLong) << 0 |
     (0xFF & buf(ofs + 1).toLong) << 8 |
     (0xFF & buf(ofs + 2).toLong) << 16 |
     (0xFF & buf(ofs + 3).toLong) << 24 |
     (0xFF & buf(ofs + 4).toLong) << 32 |
     (0xFF & buf(ofs + 5).toLong) << 40 |
     (0xFF & buf(ofs + 6).toLong) << 48 |
     (0xFF & buf(ofs + 7).toLong) << 56).toLong

  def setLong(buf: Array[Byte], ofs: Int, data: Long)(implicit order: ByteOrder) =
    if (order == ByteOrder.BIG_ENDIAN) setLongBE(buf, ofs, data) else setLongLE(buf, ofs, data)
  private def setLongBE(buf: Array[Byte], ofs: Int, data: Long): Unit = {
    buf(ofs + 0) = ((data & 0xFF00000000000000L) >> 56).toByte
    buf(ofs + 1) = ((data & 0x00FF000000000000L) >> 48).toByte
    buf(ofs + 2) = ((data & 0x0000FF0000000000L) >> 40).toByte
    buf(ofs + 3) = ((data & 0x000000FF00000000L) >> 32).toByte
    buf(ofs + 4) = ((data & 0x00000000FF000000L) >> 24).toByte
    buf(ofs + 5) = ((data & 0x0000000000FF0000L) >> 16).toByte
    buf(ofs + 6) = ((data & 0x000000000000FF00L) >> 8).toByte
    buf(ofs + 7) = ((data & 0x00000000000000FFL) >> 0).toByte
  }
  private def setLongLE(buf: Array[Byte], ofs: Int, data: Long): Unit = {
    buf(ofs + 0) = ((data & 0x00000000000000FFL) >> 0).toByte
    buf(ofs + 1) = ((data & 0x000000000000FF00L) >> 8).toByte
    buf(ofs + 2) = ((data & 0x0000000000FF0000L) >> 16).toByte
    buf(ofs + 3) = ((data & 0x00000000FF000000L) >> 24).toByte
    buf(ofs + 4) = ((data & 0x000000FF00000000L) >> 32).toByte
    buf(ofs + 5) = ((data & 0x0000FF0000000000L) >> 40).toByte
    buf(ofs + 6) = ((data & 0x00FF000000000000L) >> 48).toByte
    buf(ofs + 7) = ((data & 0xFF00000000000000L) >> 56).toByte
  }

  def readLong(is: InputStream)(implicit order: ByteOrder): Long = {
    val bytes = Array.ofDim[Byte](8)
    is.read(bytes, 0, 8)
    getLong(bytes, 0)
  }

  def writeLong(os: OutputStream, data: Long)(implicit order: ByteOrder): Unit = {
    val bytes = Array.ofDim[Byte](8)
    setLong(bytes, 0, data)
    os.write(bytes)
  }


  // doubles
  def getDouble(buf: Array[Byte], ofs: Int)(implicit order: ByteOrder): Double =
    java.lang.Double.longBitsToDouble(getLong(buf, ofs))

  def setDouble(buf: Array[Byte], ofs: Int, data: Double)(implicit order: ByteOrder): Unit = {
    setLong(buf, ofs, java.lang.Double.doubleToRawLongBits(data))
  }

  def readDouble(is: InputStream)(implicit order: ByteOrder): Double = {
    java.lang.Double.longBitsToDouble(readLong(is))
  }

  def writeDouble(os: OutputStream, data: Double)(implicit order: ByteOrder): Unit = {
    writeLong(os, java.lang.Double.doubleToRawLongBits(data))
  }
}
