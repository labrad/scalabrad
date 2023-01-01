package org.labrad.data

import java.nio.ByteOrder
import java.util.{Date, Random}
import org.labrad.types._
import org.scalatest.FunSuite

class DataTest extends FunSuite {
  val rand = new Random

  def testBothEndian(name: String)(func: ByteOrder => Unit): Unit = {
    for (byteOrder <- List(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN))
      test(name + ":" + byteOrder) { func(byteOrder) }
  }

  testBothEndian("integer") { implicit bo: ByteOrder =>
    val d = Data("i")
    for (n <- Seq(100, 0, 1, -1, Int.MaxValue, Int.MinValue)) {
      d.setInt(n)
      assert(d.getInt == n)
    }
  }

  testBothEndian("string") { implicit bo: ByteOrder =>
    val d = Data("s")
    val s = "This is a test."
    d.setString(s)
    assert(d.getString == s)
  }

  testBothEndian("date") { implicit bo: ByteOrder =>
    val d = Data("t")
    for (count <- 0 until 100000) {
      val date1 = new Date(rand.nextLong)
      d.setTime(date1)
      val date2 = d.getTime.toDate
      assert(date1 == date2)
    }
  }

  testBothEndian("string list") { implicit bo: ByteOrder =>
    val d = Data("*s")
    d.setArraySize(20)
    val it1 = d.flatIterator
    for (count <- 0 until 20)
      it1.next().setString("This is string " + count)
    val it2 = d.flatIterator
    for (count <- 0 until 20)
      assert(it2.next().getString == "This is string " + count)
  }

  testBothEndian("simple cluster") { implicit bo: ByteOrder =>
    val d1 = Data("biwsv[]c")
    val b = rand.nextBoolean
    val i = rand.nextInt
    val l = math.abs(rand.nextLong) % 4294967296L
    val s = rand.nextLong.toString
    val d = rand.nextGaussian
    val re = rand.nextGaussian
    val im = rand.nextGaussian

    val it1 = d1.clusterIterator
    it1.next().setBool(b)
    it1.next().setInt(i)
    it1.next().setUInt(l)
    it1.next().setString(s)
    it1.next().setValue(d)
    it1.next().setComplex(re, im)

    val elems = d1.clusterIterator.toSeq
    assert(b == elems(0).getBool)
    assert(i == elems(1).getInt)
    assert(l == elems(2).getUInt)
    assert(s == elems(3).getString)
    assert(d == elems(4).getValue)
    val c = elems(5).getComplex
    assert(re == c.real)
    assert(im == c.imag)
  }

  testBothEndian("list of cluster") { implicit bo: ByteOrder =>
    val d1 = Data("*(biwsv[m]c[m/s])")
    d1.setArraySize(20)
    val arrIt = d1.flatIterator
    for (count <- 0 until 20) {
      val b = rand.nextBoolean
      val i = rand.nextInt
      val l = math.abs(rand.nextLong) % 4294967296L
      val s = rand.nextLong.toString
      val d = rand.nextGaussian
      val re = rand.nextGaussian
      val im = rand.nextGaussian

      val cluster = arrIt.next()
      val it = cluster.clusterIterator
      it.next().setBool(b)
      it.next().setInt(i)
      it.next().setUInt(l)
      it.next().setString(s)
      it.next().setValue(d)
      it.next().setComplex(re, im)

      val elems = cluster.clusterIterator.toSeq
      assert(b == elems(0).getBool)
      assert(i == elems(1).getInt)
      assert(l == elems(2).getUInt)
      assert(s == elems(3).getString)
      assert(d == elems(4).getValue)
      val c = elems(5).getComplex
      assert(re == c.real)
      assert(im == c.imag)
    }

    val flat = d1.toBytes
    val d2 = Data.fromBytes(Type("*(biwsv[m]c[m/s])"), flat)
    assert(d1 == d2)
  }

  testBothEndian("multi-dimensional list") { implicit bo: ByteOrder =>
    val d1 = Arr2(
        Array(
            Array(0, 1, 2),
            Array(3, 4, 5),
            Array(6, 7, 8),
            Array(9, 10, 11)
        )
    )
    val flat = d1.toBytes
    val d2 = Data.fromBytes(Type("*2i"), flat)
    assert(d1 == d2)

    val d3 = Arr3(
        Array(
            Array(
                Array("a", "b"),
                Array("c", "d")
            ),
            Array(
                Array("e", "f"),
                Array("g", "h")
            )
        )
    )
    val flat3 = d3.toBytes
    val d4 = Data.fromBytes(Type("*3s"), flat3)
    assert(d3 == d4)
  }

  val dataStrs = Seq(
      "_",
      "true", "false",
      "+1", "-1", "+0", "-0", "+2147483647", "-2147483648",
      "1", "0", "4294967295",
      "1.0",
      "-1.0",
      "0.0",
      "2.0",
      "1e5",
      "1E5",
      "1e-1",
      "1E-1",
      "1.0E8",
      "1.0 m",
      "1.0 m/s",
      "1.0 kg*m/s^2",
      """ "abcdefgh" """,
      """ "\"\n\t' \r\f\"" """,
      " \"\\u0000\\u0001\\u0002\\u00FF\\u00ff\" ",
      """ b"\x00\x01\x02\xFF\xff" """,
      "2012-01-02T03:04:05.678Z",
      "()",
      "(_)",
      "(_,_)",
      "(((())))",
      "(1, 2.0, 3.0 m)",
      "[]",
      "[[]]",
      "[[[]]]",
      "[1, 2, 3]",
      """[
           [
             [1, 2, 3, 4],
             [5, 6, 7, 8],
             [9, 10, 11, 12],
             [13, 14, 15, 16],
             [17, 18, 19, 20],
             [21, 22, 23, 24],
             [25, 26, 27, 28],
             [29, 30, 31, 32]
           ]
         ]"""
  )

  test("data can be parsed from string representation") {
    for (s <- dataStrs) {
      val d = Data.parse(s)
      val d2 = Data.parse(d.toString)
      assert(d == d2)
    }
  }

  test("pretty-printed data can be parsed") {
    for {
      s <- dataStrs
      width <- Seq(20, 30, 40, 50, 60, 70, 80)
    } {
      val d = Data.parse(s)
      val d2 = Data.parse(d.toPrettyString(width))
      assert(d == d2)
    }
  }

  test("allow empty list to be converted to any type") {
    val empty = Arr(Array.empty[Data])
    assert(empty.t == Type("*_"))
    val c1 = empty.convertTo("*s")
    assert(c1.t == Type("*s"))

    val empty2 = Arr(Array.empty[Data])
    assert(empty2.t == Type("*_"))
    val c2 = empty2.convertTo("*?")
    assert(c2.t == Type("*_"))
  }

  test("can parse single- or double-quoted strings") {
    assert(Data.parse(""" 'this is a test' """).get[String] == """this is a test""")
    assert(Data.parse(""" "this is a test" """).get[String] == """this is a test""")
    assert(Data.parse(""" 'no "escape".' """).get[String] == """no "escape".""")
    assert(Data.parse(""" "no 'escape'." """).get[String] == """no 'escape'.""")
  }

  test("clusters of items can be parsed from map syntax") {
    val data = Data.parse(""" {"a": +1, "b": 3.0} """)
    val items = data.get[((String, Int), (String, Double))]
    assert(items == (("a", 1), ("b", 3.0)))
  }

  test("map syntax requires that all key types are the same") {
    intercept[Exception] {
      Data.parse(""" {"a": +1, 1: 3.0} """)
    }
  }

  test("clusters of items use map syntax when converted to string") {
    val data = Data.parse(""" {"a": +1, "b": 3.0} """)
    assert(data.toString == """{"a": +1, "b": 3.0}""")
  }
}
