package org.labrad.data

import java.util.Date
import org.labrad.types._

trait ToData[T] {
  def apply(b: DataBuilder, value: T): Unit

  def apply(value: T): Data = {
    val b = DataBuilder()
    apply(b, value)
    b.result()
  }
}

object ToData {
  implicit val dataToData = new ToData[Data] {
    def apply(b: DataBuilder, value: Data): Unit = b.add(value)
    override def apply(value: Data): Data = value
  }
  implicit val boolToData = new ToData[Boolean] {
    def apply(b: DataBuilder, value: Boolean): Unit = b.bool(value)
  }
  implicit val intToData = new ToData[Int] {
    def apply(b: DataBuilder, value: Int): Unit = b.int(value)
  }
  implicit val uintToData = new ToData[Long] {
    def apply(b: DataBuilder, value: Long): Unit = b.uint(value)
  }
  implicit val stringToData = new ToData[String] {
    def apply(b: DataBuilder, value: String): Unit = b.string(value)
  }
  implicit val bytesToData = new ToData[Array[Byte]] {
    def apply(b: DataBuilder, value: Array[Byte]): Unit = b.bytes(value)
  }
  implicit val dateToData = new ToData[Date] {
    def apply(b: DataBuilder, value: Date): Unit = { val t = TimeStamp(value); b.time(t.seconds, t.fraction) }
  }
  implicit def valueToData = new ToData[Double] {
    def apply(b: DataBuilder, value: Double): Unit = b.value(value)
  }
  def valueToData(unit: String) = new ToData[Double] {
    def apply(b: DataBuilder, value: Double): Unit = b.value(value, unit)
  }
  implicit def complexToData = new ToData[Complex] {
    def apply(b: DataBuilder, value: Complex): Unit = b.complex(value.real, value.imag)
  }
  def complexToData(unit: String) = new ToData[Complex] {
    def apply(b: DataBuilder, value: Complex): Unit = b.complex(value.real, value.imag, unit)
  }

  implicit def eitherToData[A, B](implicit aToData: ToData[A], bToData: ToData[B]) = new ToData[Either[A, B]] {
    def apply(b: DataBuilder, value: Either[A, B]): Unit = {
      value match {
        case Left(valueA) => b.add(valueA)
        case Right(valueB) => b.add(valueB)
      }
    }
  }

  implicit def seqToData[A](implicit aToData: ToData[A]) = new ToData[Seq[A]] {
    def apply(b: DataBuilder, value: Seq[A]): Unit = {
      b.array(value.length)
      for (elem <- value) {
        b.add(elem)
      }
    }
  }

  implicit def tuple2ToData[T1, T2](implicit s1: ToData[T1], s2: ToData[T2]) = new ToData[(T1, T2)] {
    def apply(b: DataBuilder, value: (T1, T2)): Unit = {
      value match {
        case (v1, v2) => b.clusterStart().add(v1).add(v2).clusterEnd()
      }
    }
  }

  implicit def tuple3ToData[T1, T2, T3](implicit s1: ToData[T1], s2: ToData[T2], s3: ToData[T3]) = new ToData[(T1, T2, T3)] {
    def apply(b: DataBuilder, value: (T1, T2, T3)): Unit = {
      value match {
        case (v1, v2, v3) => b.clusterStart().add(v1).add(v2).add(v3).clusterEnd()
      }
    }
  }

  implicit def tuple4ToData[T1, T2, T3, T4](implicit s1: ToData[T1], s2: ToData[T2], s3: ToData[T3], s4: ToData[T4]) = new ToData[(T1, T2, T3, T4)] {
    def apply(b: DataBuilder, value: (T1, T2, T3, T4)): Unit = {
      value match {
        case (v1, v2, v3, v4) => b.clusterStart().add(v1).add(v2).add(v3).add(v4).clusterEnd()
      }
    }
  }

  implicit def tuple5ToData[T1, T2, T3, T4, T5](implicit s1: ToData[T1], s2: ToData[T2], s3: ToData[T3], s4: ToData[T4], s5: ToData[T5]) = new ToData[(T1, T2, T3, T4, T5)] {
    def apply(b: DataBuilder, value: (T1, T2, T3, T4, T5)): Unit = {
      value match {
        case (v1, v2, v3, v4, v5) => b.clusterStart().add(v1).add(v2).add(v3).add(v4).add(v5).clusterEnd()
      }
    }
  }

  implicit def tuple6ToData[T1, T2, T3, T4, T5, T6](implicit s1: ToData[T1], s2: ToData[T2], s3: ToData[T3], s4: ToData[T4], s5: ToData[T5], s6: ToData[T6]) = new ToData[(T1, T2, T3, T4, T5, T6)] {
    def apply(b: DataBuilder, value: (T1, T2, T3, T4, T5, T6)): Unit = {
      value match {
        case (v1, v2, v3, v4, v5, v6) => b.clusterStart().add(v1).add(v2).add(v3).add(v4).add(v5).add(v6).clusterEnd()
      }
    }
  }

  implicit def tuple7ToData[T1, T2, T3, T4, T5, T6, T7](implicit s1: ToData[T1], s2: ToData[T2], s3: ToData[T3], s4: ToData[T4], s5: ToData[T5], s6: ToData[T6], s7: ToData[T7]) = new ToData[(T1, T2, T3, T4, T5, T6, T7)] {
    def apply(b: DataBuilder, value: (T1, T2, T3, T4, T5, T6, T7)): Unit = {
      value match {
        case (v1, v2, v3, v4, v5, v6, v7) => b.clusterStart().add(v1).add(v2).add(v3).add(v4).add(v5).add(v6).add(v7).clusterEnd()
      }
    }
  }

  implicit def tuple8ToData[T1, T2, T3, T4, T5, T6, T7, T8](implicit s1: ToData[T1], s2: ToData[T2], s3: ToData[T3], s4: ToData[T4], s5: ToData[T5], s6: ToData[T6], s7: ToData[T7], s8: ToData[T8]) = new ToData[(T1, T2, T3, T4, T5, T6, T7, T8)] {
    def apply(b: DataBuilder, value: (T1, T2, T3, T4, T5, T6, T7, T8)): Unit = {
      value match {
        case (v1, v2, v3, v4, v5, v6, v7, v8) => b.clusterStart().add(v1).add(v2).add(v3).add(v4).add(v5).add(v6).add(v7).add(v8).clusterEnd()
      }
    }
  }

  implicit def tuple9ToData[T1, T2, T3, T4, T5, T6, T7, T8, T9](implicit s1: ToData[T1], s2: ToData[T2], s3: ToData[T3], s4: ToData[T4], s5: ToData[T5], s6: ToData[T6], s7: ToData[T7], s8: ToData[T8], s9: ToData[T9]) = new ToData[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] {
    def apply(b: DataBuilder, value: (T1, T2, T3, T4, T5, T6, T7, T8, T9)): Unit = {
      value match {
        case (v1, v2, v3, v4, v5, v6, v7, v8, v9) => b.clusterStart().add(v1).add(v2).add(v3).add(v4).add(v5).add(v6).add(v7).add(v8).add(v9).clusterEnd()
      }
    }
  }
}
