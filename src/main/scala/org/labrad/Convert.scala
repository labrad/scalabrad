package org.labrad

import org.labrad.annotations.Setting
import org.labrad.data._
import scala.collection.mutable
import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

trait DataReader[A] {
  def read(data: Data): A
}

trait DataWriter[A] {
  def write(a: A): Data
}

trait DataFormat[A] extends DataReader[A] with DataWriter[A]

object Convert {

  implicit object UnitFormat extends DataFormat[Unit] {
    def read(data: Data): Unit = { assert(data.isNone); () }
    def write(x: Unit): Data = Data.NONE
  }

  implicit object BoolFormat extends DataFormat[Boolean] {
    def read(data: Data): Boolean = data.getBool
    def write(x: Boolean): Data = Data("b").setBool(x)
  }

  implicit object IntFormat extends DataFormat[Int] {
    def read(data: Data): Int = data.getInt
    def write(x: Int): Data = Data("i").setInt(x)
  }

  implicit object LongFormat extends DataFormat[Long] {
    def read(data: Data): Long = data.getUInt
    def write(x: Long): Data = Data("w").setUInt(x)
  }

  implicit object StringFormat extends DataFormat[String] {
    def read(data: Data): String = data.getString
    def write(s: String): Data = Data("s").setString(s)
  }

  implicit object IdFormat extends DataFormat[Data] {
    def read(data: Data): Data = data
    def write(data: Data): Data = data
  }

  implicit class SeqFormat[A](fmt: DataFormat[A]) extends DataFormat[Seq[A]] {
    def read(data: Data): Seq[A] = {
      val b = Seq.newBuilder[A]
      data.flatIterator.foreach { d => b += fmt.read(d) }
      b.result
    }
    def write(x: Seq[A]): Data = {
      Arr(x.map(fmt.write))
    }
  }

  implicit def dataFormat[A]: DataFormat[A] = macro formatImpl[A]

  def formatImpl[A](c: Context)(implicit t: c.WeakTypeTag[A]): c.Expr[DataFormat[A]] = {
    import c.universe._

    // return a list of type parameters in the given type
    // example: List[(String, Int)] => Seq(Tuple2, String, Int)
    def typeParams(tpe: Type): Seq[Type] = {
      val b = Iterable.newBuilder[Type]
      tpe.foreach(b += _)
      b.result.drop(2).grouped(2).map(_.head).toIndexedSeq
    }

    // locate an implicit DataFormat[T] for the given type
    def inferFormat(e: Tree, t: Type): Tree = {
      val writerTpe = appliedType(c.typeOf[DataFormat[_]], List(t))
      c.inferImplicitValue(writerTpe) match {
        case EmptyTree => c.abort(e.pos, s"could not find implicit value of type Writes[$t]")
        case tree => tree
      }
    }

    object Sym {
      def unapply(sym: Symbol): Option[String] = Some(sym.name.decodedName.toString)
    }

    val tree = t.tpe match {
      case t if t <:< c.typeOf[Unit] => q"UnitFormat"
      case t if t <:< c.typeOf[Boolean] => q"BoolFormat"
      case t if t <:< c.typeOf[Int] => q"IntFormat"
      case t if t <:< c.typeOf[Long] => q"LongFormat"
//      case tpe if tpe =:= typeOf[Double]  =>
//        pat match {
//          case None => Pattern("v")
//          case Some(p @ PValue(units)) => p
//          case Some(p) => sys.error(s"cannot infer type for $tpe with pattern $p")
//        }
//
//      case tpe if tpe =:= typeOf[Date]    => Pattern("t")
      case t if t <:< c.typeOf[String] => q"StringFormat"
      case t if t <:< c.typeOf[Data] => q"DataFormat"

      case TypeRef(_, Sym("Seq"), Seq(p)) if p <:< c.typeOf[Seq[_]] =>
        p match {
          case p if p <:< c.typeOf[Long] => q"SeqFormat(LongFormat)"
          case p if p <:< c.typeOf[String] => q"SeqFormat(StringFormat)"
        }
//
//      case TypeRef(_, Sym("Array") | Sym("Seq"), Seq(t)) => if (t =:= typeOf[Byte]) Pattern("s") else PArr(patternFor(t))
//
//      case TypeRef(_, Sym("Option"), Seq(t)) => Pattern.reduce(PChoice(patternFor(t), Pattern("_")))
//
//      case TypeRef(_, Sym("Either"), Seq(aL, aR)) => PChoice(patternFor(aL), patternFor(aR))
//
//      case TypeRef(_, Sym("Tuple1"), Seq(a1)) => PCluster(patternFor(a1))
//      case TypeRef(_, Sym("Tuple2"), Seq(a1, a2)) => PCluster(patternFor(a1), patternFor(a2))
//      case TypeRef(_, Sym("Tuple3"), Seq(a1, a2, a3)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3))
//      case TypeRef(_, Sym("Tuple4"), Seq(a1, a2, a3, a4)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4))
//      case TypeRef(_, Sym("Tuple5"), Seq(a1, a2, a3, a4, a5)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5))
//      case TypeRef(_, Sym("Tuple6"), Seq(a1, a2, a3, a4, a5, a6)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6))
//      case TypeRef(_, Sym("Tuple7"), Seq(a1, a2, a3, a4, a5, a6, a7)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6), patternFor(a7))
//      case TypeRef(_, Sym("Tuple8"), Seq(a1, a2, a3, a4, a5, a6, a7, a8)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6), patternFor(a7), patternFor(a8))
//      case TypeRef(_, Sym("Tuple9"), Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6), patternFor(a7), patternFor(a8), patternFor(a9))
//      case TypeRef(_, Sym("Tuple10"), Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6), patternFor(a7), patternFor(a8), patternFor(a9), patternFor(a10))
    }
    c.Expr[DataFormat[A]](tree)
  }

  implicit def writer[A]: DataWriter[A] = {
    null
  }
}
