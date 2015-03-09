package org.labrad.data

import java.util.Date
import org.labrad.types._
import scala.reflect.ClassTag

trait Getter[T] {
  // get a Data object with a value of the correct type.
  def get(data: Data): T
}

object Getter {
  implicit val dataGetter = new Getter[Data] {
    def get(data: Data): Data = data
  }
  implicit val boolGetter = new Getter[Boolean] {
    def get(data: Data): Boolean = data.getBool
  }
  implicit val intGetter = new Getter[Int] {
    def get(data: Data): Int = data.getInt
  }
  implicit val uintGetter = new Getter[Long] {
    def get(data: Data): Long = data.getUInt
  }
  implicit val stringGetter = new Getter[String] {
    def get(data: Data): String = data.getString
  }
  implicit val bytesGetter = new Getter[Array[Byte]] {
    def get(data: Data): Array[Byte] = data.getBytes
  }
  implicit val dateGetter = new Getter[Date] {
    def get(data: Data): Date = data.getTime.toDate
  }
  implicit val valueGetter = new Getter[Double] {
    def get(data: Data): Double = data.getValue
  }
  implicit val complexGetter = new Getter[Complex] {
    def get(data: Data): Complex = data.getComplex
  }

  implicit def arrayGetter[T](implicit elemGetter: Getter[T], elemTag: ClassTag[T]): Getter[Array[T]] = new Getter[Array[T]] {
    def get(data: Data): Array[T] = {
      data.flatIterator.map(elemGetter.get).toArray
    }
  }

  implicit def seqGetter[T](implicit elemGetter: Getter[T]): Getter[Seq[T]] = new Getter[Seq[T]] {
    def get(data: Data): Seq[T] = {
      data.flatIterator.map(elemGetter.get).toVector
    }
  }

  implicit def tuple2Getter[T1, T2](implicit g1: Getter[T1], g2: Getter[T2]): Getter[(T1, T2)] = new Getter[(T1, T2)] {
    def get(data: Data): (T1, T2) = {
      require(data.clusterSize == 2)
      val it = data.clusterIterator
      (g1.get(it.next), g2.get(it.next))
    }
  }

  implicit def tuple3Getter[T1, T2, T3](implicit g1: Getter[T1], g2: Getter[T2], g3: Getter[T3]): Getter[(T1, T2, T3)] = new Getter[(T1, T2, T3)] {
    def get(data: Data): (T1, T2, T3) = {
      require(data.clusterSize == 3)
      val it = data.clusterIterator
      (g1.get(it.next), g2.get(it.next), g3.get(it.next))
    }
  }

  implicit def tuple4Getter[T1, T2, T3, T4](implicit g1: Getter[T1], g2: Getter[T2], g3: Getter[T3], g4: Getter[T4]): Getter[(T1, T2, T3, T4)] = new Getter[(T1, T2, T3, T4)] {
    def get(data: Data): (T1, T2, T3, T4) = {
      require(data.clusterSize == 4)
      val it = data.clusterIterator
      (g1.get(it.next), g2.get(it.next), g3.get(it.next), g4.get(it.next))
    }
  }

  implicit def tuple5Getter[T1, T2, T3, T4, T5](implicit g1: Getter[T1], g2: Getter[T2], g3: Getter[T3], g4: Getter[T4], g5: Getter[T5]): Getter[(T1, T2, T3, T4, T5)] = new Getter[(T1, T2, T3, T4, T5)] {
    def get(data: Data): (T1, T2, T3, T4, T5) = {
      require(data.clusterSize == 5)
      val it = data.clusterIterator
      (g1.get(it.next), g2.get(it.next), g3.get(it.next), g4.get(it.next), g5.get(it.next))
    }
  }

  implicit def tuple6Getter[T1, T2, T3, T4, T5, T6](implicit g1: Getter[T1], g2: Getter[T2], g3: Getter[T3], g4: Getter[T4], g5: Getter[T5], g6: Getter[T6]): Getter[(T1, T2, T3, T4, T5, T6)] = new Getter[(T1, T2, T3, T4, T5, T6)] {
    def get(data: Data): (T1, T2, T3, T4, T5, T6) = {
      require(data.clusterSize == 6)
      val it = data.clusterIterator
      (g1.get(it.next), g2.get(it.next), g3.get(it.next), g4.get(it.next), g5.get(it.next), g6.get(it.next))
    }
  }

  implicit def tuple7Getter[T1, T2, T3, T4, T5, T6, T7](implicit g1: Getter[T1], g2: Getter[T2], g3: Getter[T3], g4: Getter[T4], g5: Getter[T5], g6: Getter[T6], g7: Getter[T7]): Getter[(T1, T2, T3, T4, T5, T6, T7)] = new Getter[(T1, T2, T3, T4, T5, T6, T7)] {
    def get(data: Data): (T1, T2, T3, T4, T5, T6, T7) = {
      require(data.clusterSize == 7)
      val it = data.clusterIterator
      (g1.get(it.next), g2.get(it.next), g3.get(it.next), g4.get(it.next), g5.get(it.next), g6.get(it.next), g7.get(it.next))
    }
  }

  implicit def tuple8Getter[T1, T2, T3, T4, T5, T6, T7, T8](implicit g1: Getter[T1], g2: Getter[T2], g3: Getter[T3], g4: Getter[T4], g5: Getter[T5], g6: Getter[T6], g7: Getter[T7], g8: Getter[T8]): Getter[(T1, T2, T3, T4, T5, T6, T7, T8)] = new Getter[(T1, T2, T3, T4, T5, T6, T7, T8)] {
    def get(data: Data): (T1, T2, T3, T4, T5, T6, T7, T8) = {
      require(data.clusterSize == 8)
      val it = data.clusterIterator
      (g1.get(it.next), g2.get(it.next), g3.get(it.next), g4.get(it.next), g5.get(it.next), g6.get(it.next), g7.get(it.next), g8.get(it.next))
    }
  }

  implicit def tuple9Getter[T1, T2, T3, T4, T5, T6, T7, T8, T9](implicit g1: Getter[T1], g2: Getter[T2], g3: Getter[T3], g4: Getter[T4], g5: Getter[T5], g6: Getter[T6], g7: Getter[T7], g8: Getter[T8], g9: Getter[T9]): Getter[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] = new Getter[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] {
    def get(data: Data): (T1, T2, T3, T4, T5, T6, T7, T8, T9) = {
      require(data.clusterSize == 9)
      val it = data.clusterIterator
      (g1.get(it.next), g2.get(it.next), g3.get(it.next), g4.get(it.next), g5.get(it.next), g6.get(it.next), g7.get(it.next), g8.get(it.next), g9.get(it.next))
    }
  }
}
