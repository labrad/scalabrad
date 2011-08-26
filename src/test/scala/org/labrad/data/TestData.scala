package org.labrad
package data

import types._

object ManagerTester {
  /**
   * Tests some of the basic functionality of the client connection.
   * This method requires that the "Python Test Server" be running
   * to complete all of its tests successfully.
   */
  def main(args: Array[String]) {
    val c = new Client
    c.connect
    
    val tags = Array(
      "b", "i", "w", "s", "t",
      
      "v", "v[]", "v[m]",
      
      "c", "c[]", "c[m/s]"
    )
    
    def dataToString(data: Data) = c.sendAndWait("Manager"){ "Data To String" -> data }(0).getString
    def stringToData(s: String) = c.sendAndWait("Manager"){ "String To Data" -> Str(s) }(0)
    
    // send random data, convert it to string, then back to data
    try {
      for (tag <- tags) {
        //val typ = Hydrant.randomType(noneOkay = false)
        val typ = Type(tag)
        println(typ)
        for (j <- 0 until 100) {
          val data1 = Hydrant.randomData(typ)
          val string1 = dataToString(data1)
          val data2 = stringToData(string1)
          val string2 = dataToString(data2)
          val data3 = stringToData(string2)
          val string3 = dataToString(data3)
          if (!(data1 approxEquals data2)) {
            println(data1.pretty)
            println(string1)
            println(data2.pretty)
            println(string2)
            println("")
          }
          assert(data1 approxEquals data2)
        }
      }
    } finally {
      c.close
    }
  }
}

object EndiannessTester {
  import java.nio.ByteOrder
  import java.nio.ByteOrder._
  
  def main(args: Array[String]) {
    def test(s: String) {
      println(s)
      val t = Type(s)
      val data = Hydrant.randomData(t)
      println("original: " + data)
      
      for (order <- List(BIG_ENDIAN, LITTLE_ENDIAN)) {
        implicit val byteOrder = order
        val bytes = data.toBytes
        val unflattened = Data.fromBytes(bytes, t)
      
        println("flattened (" + byteOrder + "): " + bytes.mkString(","))
        println("unflattened (" + byteOrder + "): " + unflattened)
        assert(unflattened == data)
      }
      println
    }
    
    test("i")
    test("w")
    test("v")
    test("c")
    test("t")
    test("s")
    test("ii")
    test("cvtiwsbb")
    test("*(is)")
    test("*2(i*s)")
  }
}
