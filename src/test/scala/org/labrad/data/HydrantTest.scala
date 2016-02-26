package org.labrad.data

import java.nio.ByteOrder
import org.labrad.types._
import org.scalatest.FunSuite

class HydrantTest extends FunSuite {

  def testHydrant(tag: String) = test(tag) {
    val t = Type(tag)
    val data = Hydrant.randomData(t)

    for (order <- List(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)) {
      implicit val bo = order

      val bytes = data.toBytes
      val unflattened = Data.fromBytes(data.t, bytes)
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
