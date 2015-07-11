package org.labrad.data

import java.nio.ByteOrder
import java.util.{Date, Random}
import org.labrad.types._
import org.scalatest.FunSuite

class DataBuilderTest extends FunSuite {
  val rand = new Random

  def testBothEndian(name: String)(func: ByteOrder => Unit) {
    for (byteOrder <- List(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN))
      test(name + ":" + byteOrder) { func(byteOrder) }
  }

  testBothEndian("integer") { implicit bo: ByteOrder =>
    for (n <- Seq(100, 0, 1, -1, Int.MaxValue, Int.MinValue)) {
      val d1 = DataBuilder().int(n).result()
      val d2 = DataBuilder("i").int(n).result()
      assert(d1.t == TInt)
      assert(d2.t == TInt)
      assert(d1.getInt == n)
      assert(d2.getInt == n)
    }
  }

  testBothEndian("string") { implicit bo: ByteOrder =>
    val s = "This is a test."
    val d1 = DataBuilder().string(s).result()
    val d2 = DataBuilder().string(s).result()
    assert(d1.t == TStr)
    assert(d2.t == TStr)
    assert(d1.getString == s)
    assert(d2.getString == s)
  }

  testBothEndian("date") { implicit bo: ByteOrder =>
    for (count <- 0 until 100000) {
      val date = new Date(rand.nextLong)
      val timestamp = TimeStamp(date)
      val d1 = DataBuilder().time(timestamp.seconds, timestamp.fraction).result()
      val d2 = DataBuilder().time(timestamp.seconds, timestamp.fraction).result()
      assert(d1.t == TTime)
      assert(d2.t == TTime)
      assert(d1.getTime.toDate == date)
      assert(d2.getTime.toDate == date)
    }
  }

  testBothEndian("string list") { implicit bo: ByteOrder =>
    def check(b: DataBuilder): Unit = {
      b.array(20)
      for (count <- 0 until 20) {
        b.string(s"This is string $count")
      }
      val d = b.result()
      val it = d.flatIterator
      for (count <- 0 until 20) {
        assert(it.next.getString == s"This is string $count")
      }
    }
    check(DataBuilder())
    check(DataBuilder("*s"))
  }

  test("DataBuilder enforces that arrays are homogeneous") {
    val b = DataBuilder()
    b.array(2)
    b.int(1)
    intercept[Exception] { b.string("oops!") }
  }

  test("cluster fails to build if not wrong number of elements are provided") {
    val b = DataBuilder("(ss)")
    b.clusterStart()
    intercept[Exception] { b.clusterEnd() }
    b.string("a")
    intercept[Exception] { b.clusterEnd() }
    b.string("b")
    intercept[Exception] { b.string("c") }
    b.clusterEnd()
    b.result()
  }

  test("array elem type inferred from first element") {
    val b = DataBuilder()
    b.array(2)
     .clusterStart()
       .string("a")
       .int(1)
     .clusterEnd()

    // now the type is *(si) so we expect (si) for the next element
    intercept[Exception] { b.int(1) }
    b.clusterStart()
    intercept[Exception] { b.clusterEnd() }
    intercept[Exception] { b.int(1) }
    b.string("b")
    intercept[Exception] { b.clusterEnd() }
    b.int(2)
    intercept[Exception] { b.string("c") }
    b.clusterEnd()
    b.result()
  }

  test("multi-dimensional array accepts correct number of elements") {
    val b = DataBuilder()
    b.array(2, 3)
     .int(11)
     .int(12)
     .int(13)
     .int(21)
     .int(22)
     .int(23)
    intercept[Exception] { b.int(31) }
    b.result()
  }

  test("empty array should get type *_") {
    val d = DataBuilder().array(0).result()
    assert(d.t == TArr(TNone, depth = 1))

    val d2 = DataBuilder().array(1, 0).result()
    assert(d2.t == TArr(TNone, depth = 2))
  }

  test("empty array of known type is allowed") {
    val d = DataBuilder("*s").array(0).result()
    assert(d.t == TArr(TStr, depth = 1))

    val d2 = DataBuilder("*2s").array(1, 0).result()
    assert(d2.t == TArr(TStr, depth = 2))
  }

  test("empty array type can be narrowed in subsequent elements") {
    val d = DataBuilder()
      .array(2)
        .clusterStart()
          .array(0)
        .clusterEnd()
        .clusterStart()
          .array(1).string("hi")
        .clusterEnd()
      .result()
    assert(d.t == Type("*(*s)"))

    val d2 = DataBuilder()
      .array(3)
        .clusterStart()
          .array(0)
          .array(0)
        .clusterEnd()
        .clusterStart()
          .array(1).string("hi")
          .array(0)
        .clusterEnd()
        .clusterStart()
          .array(0)
          .array(1).int(2)
        .clusterEnd()
      .result()
    assert(d2.t == Type("*(*s*i)"))
  }

  test("none is not allowed in array") {
    val b = DataBuilder().array(1)
    intercept[Exception] { b.none() }
    b.int(1).result()
  }
}
