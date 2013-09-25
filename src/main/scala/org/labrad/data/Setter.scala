package org.labrad.data

import java.util.Date
import org.labrad.types._

trait Setter[T] {
  // the LabRAD type that this setter sets.
  def t: Type

  // set a Data object with a value of the correct type.
  def set(data: Data, value: T): Unit
}

object Setters {
  val boolSetter = new Setter[Boolean] {
    val t = TBool
    def set(data: Data, value: Boolean) { data.setBool(value) }
  }
  val intSetter = new Setter[Int] {
    val t = TInt
    def set(data: Data, value: Int) { data.setInt(value) }
  }
  val uintSetter = new Setter[Long] {
    val t = TUInt
    def set(data: Data, value: Long) { data.setUInt(value) }
  }
  val stringSetter = new Setter[String] {
    val t = TStr
    def set(data: Data, value: String) { data.setString(value) }
  }
  val dateSetter = new Setter[Date] {
    val t = TTime
    def set(data: Data, value: Date) { data.setTime(value) }
  }
  def valueSetter = new Setter[Double] {
    val t = TValue()
    def set(data: Data, value: Double) { data.setValue(value) }
  }
  def valueSetter(units: String = "") = new Setter[Double] {
    val t = TValue(units)
    def set(data: Data, value: Double) { data.setValue(value) }
  }
  def complexSetter = new Setter[Complex] {
    val t = TComplex()
    def set(data: Data, value: Complex) { data.setComplex(value) }
  }
  def complexSetter(units: String = "") = new Setter[Complex] {
    val t = TComplex(units)
    def set(data: Data, value: Complex) { data.setComplex(value) }
  }
}
