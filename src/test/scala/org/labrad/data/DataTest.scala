package org.labrad.data

import java.nio.ByteOrder
import java.util.{Date, Random}
import org.labrad.types._
import org.scalatest.FunSuite

class DataTests extends FunSuite {
  val rand = new Random

  def testBothEndian(name: String)(func: ByteOrder => Unit) {
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
    for (count <- 0 until 20)
      d(count).setString("This is string " + count)
    for (count <- 0 until 20)
      assert(d(count).getString == "This is string " + count)
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

    d1(0).setBool(b)
    d1(1).setInt(i)
    d1(2).setUInt(l)
    d1(3).setString(s)
    d1(4).setValue(d)
    d1(5).setComplex(re, im)

    assert(b == d1(0).getBool)
    assert(i == d1(1).getInt)
    assert(l == d1(2).getUInt)
    assert(s == d1(3).getString)
    assert(d == d1(4).getValue)
    val c = d1(5).getComplex
    assert(re == c.real)
    assert(im == c.imag)
  }

  testBothEndian("list of cluster") { implicit bo: ByteOrder =>
    val d1 = Data("*(biwsv[m]c[m/s])")
    d1.setArraySize(20)
    for (count <- 0 until 20) {
      val b = rand.nextBoolean
      val i = rand.nextInt
      val l = math.abs(rand.nextLong) % 4294967296L
      val s = rand.nextLong.toString
      val d = rand.nextGaussian
      val re = rand.nextGaussian
      val im = rand.nextGaussian

      d1(count, 0).setBool(b)
      d1(count, 1).setInt(i)
      d1(count, 2).setUInt(l)
      d1(count, 3).setString(s)
      d1(count, 4).setValue(d)
      d1(count, 5).setComplex(re, im)

      assert(b == d1(count, 0).getBool)
      assert(i == d1(count, 1).getInt)
      assert(l == d1(count, 2).getUInt)
      assert(s == d1(count, 3).getString)
      assert(d == d1(count, 4).getValue)
      val c = d1(count, 5).getComplex
      assert(re == c.real)
      assert(im == c.imag)
    }

    val flat = d1.toBytes
    val d2 = Data.fromBytes(flat, Type("*(biwsv[m]c[m/s])"))
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
    val d2 = Data.fromBytes(flat, Type("*2i"))
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
    val d4 = Data.fromBytes(flat3, Type("*3s"))
    assert(d3 == d4)
  }

  test("data can be parsed from string representation") {
    val strs = Seq(
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
        """ "\x00\x01\x02\xFF\xff" """,
        "2012-01-02T03:04:05.678Z",
        "()",
        "(_)",
        "(_,_)",
        "(((())))",
        "(1, 2.0, 3.0 m)",
        "[]",
        "[[]]",
        "[[[]]]",
        "[1, 2, 3]"
    )

    strs foreach { s =>
      val d = Data.parse(s)
      val d2 = Data.parse(d.toString)
      assert(d == d2)
      //assert(d ~== d2)
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
}


class HydrantTests extends FunSuite {

  def testHydrant(tag: String) = test(tag) {
    val t = Type(tag)
    val data = Hydrant.randomData(t)

    for (order <- List(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)) {
      implicit val bo = order

      val bytes = data.toBytes
      val unflattened = Data.fromBytes(bytes, data.t)
      assert(unflattened == data)
    }
  }

  val types = Seq(
    "i",
    "w",
    "v",
    "c",
    "t",
    "s",
    "ii",
    "cvtiwsbb",
    "*(is)",
    "*2(i*s)"
  )

  for (tag <- types)
    testHydrant(tag)
}
