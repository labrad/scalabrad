package org.labrad.data

import java.nio.ByteOrder
import java.nio.ByteOrder._

object DataBench {

  def timeIt[A](msg: String, noisy: Boolean = true, reps: Int = 1000)(f: => A): (A, Long) = {
    for (i <- 0 until 10) { f }
    val start = System.nanoTime()
    val result = f
    for (_ <- 0 until reps-1) {
      f
    }
    val end = System.nanoTime()
    val elapsed = end - start
    if (noisy) {
      println(s"$msg: elapsed = ${elapsed / 1.0e3 / reps} us")
    }
    (result, elapsed)
  }

  def bench(data: Data): Unit = {
    val bytesBE = data.toBytes(BIG_ENDIAN)
    val bytesLE = data.toBytes(LITTLE_ENDIAN)

    val unflatBE = TreeData.fromBytes(data.t, bytesBE)(BIG_ENDIAN)
    val unflatLE = TreeData.fromBytes(data.t, bytesLE)(LITTLE_ENDIAN)

    val flatBE = FlatData.fromBytes(data.t, bytesBE)(BIG_ENDIAN)
    val flatLE = FlatData.fromBytes(data.t, bytesLE)(LITTLE_ENDIAN)

    timeIt("flatten") { unflatBE.toBytes(BIG_ENDIAN) }
    timeIt("flattenFlat") { flatBE.toBytes(BIG_ENDIAN) }
    println()

    timeIt("flattenSwap") { unflatBE.toBytes(LITTLE_ENDIAN) }
    timeIt("flattenFlatSwap") { flatBE.toBytes(BIG_ENDIAN) }
    println()

    timeIt("unflattenBE") { TreeData.fromBytes(data.t, bytesBE)(BIG_ENDIAN) }
    timeIt("unflattenFlatBE") { FlatData.fromBytes(data.t, bytesBE)(BIG_ENDIAN) }
    println()

    timeIt("unflattenLE") { TreeData.fromBytes(data.t, bytesLE)(LITTLE_ENDIAN) }
    timeIt("unflattenFlatLE") { FlatData.fromBytes(data.t, bytesLE)(LITTLE_ENDIAN) }
    println()
  }

  def benchGet[A](msg: String, data: Data, get: Data => A): Unit = {
    val bytesBE = data.toBytes(BIG_ENDIAN)
    val bytesLE = data.toBytes(LITTLE_ENDIAN)

    val treeBE = TreeData.fromBytes(data.t, bytesBE)(BIG_ENDIAN)
    val treeLE = TreeData.fromBytes(data.t, bytesLE)(LITTLE_ENDIAN)

    val flatBE = FlatData.fromBytes(data.t, bytesBE)(BIG_ENDIAN)
    val flatLE = FlatData.fromBytes(data.t, bytesLE)(LITTLE_ENDIAN)

    timeIt(s"${msg}:getBE", reps = 100) { get(treeBE) }
    timeIt(s"${msg}:getFlatBE", reps = 100) { get(flatBE) }
    println()

    timeIt(s"${msg}:getLE", reps = 100) { get(treeLE) }
    timeIt(s"${msg}:getFlatLE", reps = 100) { get(flatLE) }
    println()
  }

  def benchSet(msg: String, set: ByteOrder => Data): Unit = {
    timeIt(s"${msg}:BE", reps = 100) { set(BIG_ENDIAN) }
    timeIt(s"${msg}:LE", reps = 100) { set(LITTLE_ENDIAN) }
    println()
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

    val data2 = Arr(Seq.fill(N) {
      Cluster(
        Str("01:23:45:67:89:AB"),
        Str("01:23:45:67:89:AB"),
        Integer(128),
        Bytes(Array.tabulate(128)(_.toByte)))
      }
    )

    assert(data1 == data2)
    assert(data1.toBytes(BIG_ENDIAN).toSeq == data2.toBytes(BIG_ENDIAN).toSeq)

    val intArray = Array.tabulate[Int](100000) { i => i }
    val intArrayData = Arr(intArray)
    val doubleArray = Array.tabulate[Double](100000) { i => i.toDouble }
    val doubleArrayData = Arr(doubleArray)

    println("cluster array:")
    bench(data1)
    println()

    println("int array:")
    bench(intArrayData)
    benchGet("getFlatIter", intArrayData, data => {
      data.flatIterator.map(_.get[Int]).toArray
    })
    benchGet("getFast", intArrayData, data => {
      val bytes = data.arrayBytes
      val n = data.arraySize
      assert(bytes.length == 4*n)
      val result = Array.ofDim[Int](n)
      var i = 0
      while (i < n) {
        result(i) = bytes.getInt(4*i)
        i += 1
      }
      result
    })
    benchGet("get[Array[Int]]", intArrayData, _.get[Array[Int]])
    benchSet("set:Arr(ints)", implicit endianness => {
      Arr(intArray)
    })
    benchSet("set:fast", implicit endianness => {
      val data = Data("*i")
      val n = intArray.length
      data.setArrayShape(n)
      val bytes = data.arrayBytes
      var i = 0
      while (i < n) {
        bytes.setInt(4 * i, intArray(i))
        i += 1
      }
      data
    })
    println()

    println("double array:")
    bench(doubleArrayData)
    benchGet("getFlatIter", doubleArrayData, data => {
      data.flatIterator.map(_.get[Double]).toArray
    })
    benchGet("getFast", doubleArrayData, data => {
      val bytes = data.arrayBytes
      val n = data.arraySize
      assert(bytes.length == 8*n)
      val result = Array.ofDim[Double](n)
      var i = 0
      while (i < n) {
        result(i) = bytes.getDouble(8*i)
        i += 1
      }
      result
    })
    benchGet("get[Array[Double]]", doubleArrayData, _.get[Array[Double]])
    benchSet("Arr(doubles)", implicit endianness => {
      Arr(doubleArray)
    })
    benchSet("set:fast", implicit endianness => {
      val data = Data("*v")
      val n = doubleArray.length
      data.setArrayShape(n)
      val bytes = data.arrayBytes
      var i = 0
      while (i < n) {
        bytes.setDouble(8 * i, doubleArray(i))
        i += 1
      }
      data
    })
    println()
  }
}
