package org.labrad.data

import io.netty.buffer.{ByteBuf, Unpooled}
import java.nio.ByteOrder._

object DataBench {

  def timeIt[A](msg: String, noisy: Boolean = true)(f: => A): (A, Long) = {
    for (i <- 0 until 10) { f }
    val start = System.nanoTime()
    val result = f
    val end = System.nanoTime()
    val elapsed = end - start
    if (noisy) {
      println(s"$msg: elapsed = ${elapsed/1.0e9} ms")
    }
    (result, elapsed)
  }

  def main(args: Array[String]): Unit = {
    //val data = Str("howdy!")

    val data = Arr(Seq.fill(512) { Cluster(Str("01:23:45:67:89:AB"), Str("01:23:45:67:89:AB"), Integer(128), Bytes(Array.tabulate(128)(_.toByte))) })

    val bytesBE = data.toBytes(BIG_ENDIAN)
    val bytesLE = data.toBytes(LITTLE_ENDIAN)

    val flatBE = new FlatData(data.t, bytesBE, 0)(BIG_ENDIAN)
    val flatLE = new FlatData(data.t, bytesLE, 0)(LITTLE_ENDIAN)

    timeIt("flatten") {
      for (i <- 0 until 1000) {
        data.toBytes(BIG_ENDIAN)
      }
    }
    timeIt("flattenFlat") {
      for (i <- 0 until 1000) {
        flatBE.toBytes(BIG_ENDIAN)
      }
    }
    println()

    timeIt("flattenSwap") {
      for (i <- 0 until 1000) {
        data.toBytes(LITTLE_ENDIAN)
      }
    }
    timeIt("flattenFlatSwap") {
      for (i <- 0 until 1000) {
        flatBE.toBytes(BIG_ENDIAN)
      }
    }
    println()

    timeIt("unflattenBE") {
      for (i <- 0 until 1000) {
        Data.fromBytes(bytesBE, data.t)(BIG_ENDIAN)
      }
    }
    timeIt("unflattenFlatBE") {
      for (i <- 0 until 1000) {
        val fd = new FlatData(data.t, bytesBE, 0)(BIG_ENDIAN)
        assert(fd.len == bytesBE.length)
      }
    }
    println()

    timeIt("unflattenLE") {
      for (i <- 0 until 1000) {
        Data.fromBytes(bytesLE, data.t)(LITTLE_ENDIAN)
      }
    }
    timeIt("unflattenFlatLE") {
      for (i <- 0 until 1000) {
        val fd = new FlatData(data.t, bytesLE, 0)(LITTLE_ENDIAN)
        assert(fd.len == bytesLE.length)
      }
    }
  }
}
