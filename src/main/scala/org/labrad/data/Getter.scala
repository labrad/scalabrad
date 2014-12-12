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
}
