package org.labrad.data

import java.util.Date
import org.labrad.types._

trait Setter[T] {
  // set a Data object with a value of the correct type.
  def set(data: Data, value: T): Unit
}

object Setter {
  implicit val dataSetter = new Setter[Data] {
    def set(data: Data, value: Data) { data.set(value) }
  }
  implicit val boolSetter = new Setter[Boolean] {
    def set(data: Data, value: Boolean) { data.setBool(value) }
  }
  implicit val intSetter = new Setter[Int] {
    def set(data: Data, value: Int) { data.setInt(value) }
  }
  implicit val uintSetter = new Setter[Long] {
    def set(data: Data, value: Long) { data.setUInt(value) }
  }
  implicit val stringSetter = new Setter[String] {
    def set(data: Data, value: String) { data.setString(value) }
  }
  implicit val dateSetter = new Setter[Date] {
    def set(data: Data, value: Date) { data.setTime(value) }
  }
  implicit def valueSetter = new Setter[Double] {
    def set(data: Data, value: Double) { data.setValue(value) }
  }
  implicit def complexSetter = new Setter[Complex] {
    def set(data: Data, value: Complex) { data.setComplex(value) }
  }
}
