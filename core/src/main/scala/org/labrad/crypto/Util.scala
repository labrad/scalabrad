package org.labrad.crypto

import scala.language.implicitConversions

import java.math.BigInteger

object BigInts {
  object Implicits {
    implicit def bigIntToBigInteger(n: BigInt): BigInteger = n.bigInteger

    implicit class RichBigInt(val x: BigInt) extends AnyVal {
      def toUnsignedByteArray: Array[Byte] = {
        val bytes = x.toByteArray
        if (bytes.length >= 2 && bytes(0) == 0) bytes.tail else bytes
      }
    }

    implicit class RichBigInteger(val x: BigInteger) extends AnyVal {
      def toUnsignedByteArray: Array[Byte] = {
        val bytes = x.toByteArray
        if (bytes.length >= 2 && bytes(0) == 0) bytes.tail else bytes
      }
    }
  }

  def toUnsignedByteArray(b: BigInteger): Array[Byte] = {
    val bytes = b.toByteArray
    if (bytes.length >= 2 && bytes(0) == 0) bytes.tail else bytes
  }

  def toUnsignedByteArray(b: BigInt): Array[Byte] = {
    toUnsignedByteArray(b.bigInteger)
  }

  def fromUnsignedByteArray(bytes: Array[Byte]): BigInt = {
    BigInt(0.toByte +: bytes)
  }
}
