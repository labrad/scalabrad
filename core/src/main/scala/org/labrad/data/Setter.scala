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

  implicit def tuple2Setter[T1, T2](implicit s1: Setter[T1], s2: Setter[T2]) = new Setter[(T1, T2)] {
    def set(data: Data, value: (T1, T2)) {
      require(data.clusterSize == 2)
      val it = data.clusterIterator
      s1.set(it.next, value._1)
      s2.set(it.next, value._2)
    }
  }

  implicit def tuple3Setter[T1, T2, T3](implicit s1: Setter[T1], s2: Setter[T2], s3: Setter[T3]) = new Setter[(T1, T2, T3)] {
    def set(data: Data, value: (T1, T2, T3)) {
      require(data.clusterSize == 3)
      val it = data.clusterIterator
      s1.set(it.next, value._1)
      s2.set(it.next, value._2)
      s3.set(it.next, value._3)
    }
  }

  implicit def tuple4Setter[T1, T2, T3, T4](implicit s1: Setter[T1], s2: Setter[T2], s3: Setter[T3], s4: Setter[T4]) = new Setter[(T1, T2, T3, T4)] {
    def set(data: Data, value: (T1, T2, T3, T4)) {
      require(data.clusterSize == 4)
      val it = data.clusterIterator
      s1.set(it.next, value._1)
      s2.set(it.next, value._2)
      s3.set(it.next, value._3)
      s4.set(it.next, value._4)
    }
  }

  implicit def tuple5Setter[T1, T2, T3, T4, T5](implicit s1: Setter[T1], s2: Setter[T2], s3: Setter[T3], s4: Setter[T4], s5: Setter[T5]) = new Setter[(T1, T2, T3, T4, T5)] {
    def set(data: Data, value: (T1, T2, T3, T4, T5)) {
      require(data.clusterSize == 5)
      val it = data.clusterIterator
      s1.set(it.next, value._1)
      s2.set(it.next, value._2)
      s3.set(it.next, value._3)
      s4.set(it.next, value._4)
      s5.set(it.next, value._5)
    }
  }

  implicit def tuple6Setter[T1, T2, T3, T4, T5, T6](implicit s1: Setter[T1], s2: Setter[T2], s3: Setter[T3], s4: Setter[T4], s5: Setter[T5], s6: Setter[T6]) = new Setter[(T1, T2, T3, T4, T5, T6)] {
    def set(data: Data, value: (T1, T2, T3, T4, T5, T6)) {
      require(data.clusterSize == 6)
      val it = data.clusterIterator
      s1.set(it.next, value._1)
      s2.set(it.next, value._2)
      s3.set(it.next, value._3)
      s4.set(it.next, value._4)
      s5.set(it.next, value._5)
      s6.set(it.next, value._6)
    }
  }

  implicit def tuple7Setter[T1, T2, T3, T4, T5, T6, T7](implicit s1: Setter[T1], s2: Setter[T2], s3: Setter[T3], s4: Setter[T4], s5: Setter[T5], s6: Setter[T6], s7: Setter[T7]) = new Setter[(T1, T2, T3, T4, T5, T6, T7)] {
    def set(data: Data, value: (T1, T2, T3, T4, T5, T6, T7)) {
      require(data.clusterSize == 7)
      val it = data.clusterIterator
      s1.set(it.next, value._1)
      s2.set(it.next, value._2)
      s3.set(it.next, value._3)
      s4.set(it.next, value._4)
      s5.set(it.next, value._5)
      s6.set(it.next, value._6)
      s7.set(it.next, value._7)
    }
  }

  implicit def tuple8Setter[T1, T2, T3, T4, T5, T6, T7, T8](implicit s1: Setter[T1], s2: Setter[T2], s3: Setter[T3], s4: Setter[T4], s5: Setter[T5], s6: Setter[T6], s7: Setter[T7], s8: Setter[T8]) = new Setter[(T1, T2, T3, T4, T5, T6, T7, T8)] {
    def set(data: Data, value: (T1, T2, T3, T4, T5, T6, T7, T8)) {
      require(data.clusterSize == 8)
      val it = data.clusterIterator
      s1.set(it.next, value._1)
      s2.set(it.next, value._2)
      s3.set(it.next, value._3)
      s4.set(it.next, value._4)
      s5.set(it.next, value._5)
      s6.set(it.next, value._6)
      s7.set(it.next, value._7)
      s8.set(it.next, value._8)
    }
  }

  implicit def tuple9Setter[T1, T2, T3, T4, T5, T6, T7, T8, T9](implicit s1: Setter[T1], s2: Setter[T2], s3: Setter[T3], s4: Setter[T4], s5: Setter[T5], s6: Setter[T6], s7: Setter[T7], s8: Setter[T8], s9: Setter[T9]) = new Setter[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] {
    def set(data: Data, value: (T1, T2, T3, T4, T5, T6, T7, T8, T9)) {
      require(data.clusterSize == 9)
      val it = data.clusterIterator
      s1.set(it.next, value._1)
      s2.set(it.next, value._2)
      s3.set(it.next, value._3)
      s4.set(it.next, value._4)
      s5.set(it.next, value._5)
      s6.set(it.next, value._6)
      s7.set(it.next, value._7)
      s8.set(it.next, value._8)
      s9.set(it.next, value._9)
    }
  }
}
