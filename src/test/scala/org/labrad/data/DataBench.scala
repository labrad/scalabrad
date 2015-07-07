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
    val N = 512

    val data1 = {
      val b = DataBuilder()
      b.array(N)
      for (_ <- 0 until N) {
        b.clusterStart()
           .string("01:23:45:67:89:AB")
           .string("01:23:45:67:89:AB")
           .int(128)
           .bytes(Array.tabulate(128)(_.toByte))
         .clusterEnd()
      }
      b.result()
    }

    val data2 = Arr(Seq.fill(N) { Cluster(Str("01:23:45:67:89:AB"), Str("01:23:45:67:89:AB"), Integer(128), Bytes(Array.tabulate(128)(_.toByte))) })

    assert(data1 == data2)
    assert(data1.toBytes(BIG_ENDIAN).toSeq == data2.toBytes(BIG_ENDIAN).toSeq)

    val data = data2

    val bytesBE = data1.toBytes(BIG_ENDIAN)
    val bytesLE = data1.toBytes(LITTLE_ENDIAN)

    val unflatBE = TreeData.fromBytes(data.t, bytesBE)(BIG_ENDIAN)
    val unflatLE = TreeData.fromBytes(data.t, bytesLE)(LITTLE_ENDIAN)

    val flatBE = FlatData.fromBytes(data.t, bytesBE)(BIG_ENDIAN)
    val flatLE = FlatData.fromBytes(data.t, bytesLE)(LITTLE_ENDIAN)

    timeIt("flatten") {
      for (i <- 0 until 1000) {
        unflatBE.toBytes(BIG_ENDIAN)
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
        unflatBE.toBytes(LITTLE_ENDIAN)
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
        TreeData.fromBytes(data.t, bytesBE)(BIG_ENDIAN)
      }
    }
    timeIt("unflattenFlatBE") {
      for (i <- 0 until 1000) {
        FlatData.fromBytes(data.t, bytesBE)(BIG_ENDIAN)
      }
    }
    println()

    timeIt("unflattenLE") {
      for (i <- 0 until 1000) {
        TreeData.fromBytes(data.t, bytesLE)(LITTLE_ENDIAN)
      }
    }
    timeIt("unflattenFlatLE") {
      for (i <- 0 until 1000) {
        FlatData.fromBytes(data.t, bytesLE)(LITTLE_ENDIAN)
      }
    }
  }
}
