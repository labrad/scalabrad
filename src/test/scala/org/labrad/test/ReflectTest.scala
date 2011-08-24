package org.labrad
package test

import java.lang.{Double => JDouble}
import java.util.{Map => JMap}

import data._

class ReflectTest {
  def basicTypes(
      a: Boolean,
      b: Int,
      c: Long,
      d: Double,
      e: String,
      f: Data) {
    println("basicTypes called")
  }
  
  def arrayTypes(
      a: Array[Boolean],
      b: Array[Byte],
      c: Array[Int],
      d: Array[Long],
      e: Array[String],
      f: Array[Double],
      g: Array[Data]
      ) {
    println("arrayTypes called")
  }
  
  def seqTypes(
      a: Seq[Boolean],
      b: Seq[Byte],
      c: Seq[Int],
      d: Seq[Long],
      e: Seq[String],
      f: Seq[Double],
      g: Seq[Data]
      ) {
    println("seqTypes called")
  }
  
  def optionTypes(
      a: Option[Boolean],
      b: Option[Byte],
      c: Option[Int],
      d: Option[Long],
      e: Option[String],
      f: Option[Double],
      g: Option[Data]
      ) {
    println("optionTypes called")
  }
  
  def eitherTypes(
      a: Either[Boolean, Int],
      b: Either[Long, String]
      ) {
    println("eitherTypes called")
  }
  
  def tupleTypes(
      a: Tuple1[Boolean],
      b: (Boolean, Int),
      c: (Boolean, Int, Long),
      d: (Boolean, Int, Long, String),
      e: (Boolean, Int, Long, String, Double)
      ) {
    println("tupleTypes called")
  }
  
  def repeatedBoolean(a: Boolean*) {}
  
  def byNameTypes(
      a: => Boolean
      ) {
    println("byNameTypes called")
  }
  
  def foo(a: Double, b: String, c: JMap[Double, String]) {
    println("foo called")
  }
  
  def bar(a: Double, b: String, c: Map[Double, String]) {
    println("bar called")
  }
  
  def baz(a: Double, b: String, c: JMap[JDouble, String]) {
    println("foo called")
  }

  def qux(a: Double, b: String, c: Map[JDouble, String]) {
    println("foo called")
  }
  
  def blzh(a: List[Int]) {}
}

object ReflectTest extends App {
  val rt= new ReflectTest
  rt.foo(1, "2", null)
}
