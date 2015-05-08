package org.labrad

import java.lang.reflect.InvocationTargetException
import java.util.Date
import org.labrad.annotations.Setting
import org.labrad.annotations.Matchers._
import org.labrad.data._
import org.labrad.types.{Pattern, PArr, PChoice, PCluster, PValue, PComplex, TNone}
import org.labrad.util.Logging
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe.{Return => _, _}
import scala.reflect.runtime.currentMirror

case class TypeInfo(pat: Pattern, strs: Seq[String])
object TypeInfo {
  def fromPatterns(pats: Seq[String]): TypeInfo = {
    val pattern = pats match {
      case Seq()  => Pattern("?")
      case Seq(p) => Pattern(p)
      case ps     => Pattern.reduce(ps.map(Pattern(_)): _*)
    }
    TypeInfo(pattern, pats)
  }
}


case class SettingInfo(id: Long, name: String, doc: String, accepts: TypeInfo, returns: TypeInfo)

case class ServerInfo(id: Long, name: String, doc: String, settings: Seq[SettingInfo]) {
  def setting(id: Long): Option[SettingInfo] = settings.find(_.id == id)
  def setting(name: String): Option[SettingInfo] = settings.find(_.name == name)
}

case class Handler(f: PartialFunction[Data, Data], accepts: Pattern = Pattern("?"), returns: Pattern = Pattern("?"))

object SettingHandler extends Logging {
  // TODO handle annotated types such as units
  // TODO if there is an annotation and inferred type, make sure they are compatible

  /** Get metadata and an unbound handler function for a sequence of overridden methods */
  def forMethods(methods: Seq[(MethodSymbol, Seq[MethodSymbol])]): (Pattern, Pattern, Any => PartialFunction[RequestContext, Data]) = {
    require(methods.size > 0, "must specify at least one method")

    val (accepts, returns, binders) = methods.map { case (m, defaults) => makeHandler(m, defaults) }.unzip3

    // create a binder that binds all overloads, then tries each one in sequence
    val binder = (self: Any) => binders.map(_(self)).reduceLeft(_ orElse _)

    (Pattern.reduce(accepts: _*), Pattern.reduce(returns: _*), binder)
  }

  /** Get metadata and an unbound handler function for a method */
  def makeHandler(m: MethodSymbol, defaultMethods: Seq[MethodSymbol]): (Pattern, Pattern, Any => PartialFunction[RequestContext, Data]) = {
    val (types, returnType) = m.typeSignature match {
      case PolyType(args, ret) => (args.map(_.asType), ret)
      case MethodType(args, ret) => (args.map(_.asTerm), ret)
      case NullaryMethodType(ret) => (Nil, ret)
    }

    // check if the first argument is a RequestContext
    val includeRequestContext = types.nonEmpty && types(0).typeSignature <:< typeOf[RequestContext]
    val unpackTypes = if (includeRequestContext) types.tail else types
    val firstUnpackParam = if (includeRequestContext) 2 else 1

    // a sequence of optional defaults for every unpackable parameter of the method,
    // skipping the first parameter if it is the raw RequestContext
    val defaults = for (i <- firstUnpackParam to types.length) yield
      defaultMethods.find(_.name.decodedName.toString == m.name.decodedName.toString + "$default$" + i)

    // get patterns and unpackers for all method parameters
    val (patterns, unpackers) = unpackTypes.map(t => inferParamType(t.typeSignature, t.annotations)).unzip

    // modify argument patterns to allow none if the argument has a default
    val defaultPatterns = for ((p, defaultOpt) <- patterns zip defaults) yield
      if (defaultOpt.isDefined) Pattern.reduce(p, TNone) else p

    // create patterns for combined arguments, including with trailing optional args dropped
    // each pattern is combined in a tuple with the number of function arguments included in
    // the pattern and the number of function arguments omitted from the pattern
    val acceptPats = defaultPatterns match {
      case Seq()  => Seq((TNone, 0, 0))
      case Seq(p) => Seq((p, 1, 0))
      case ps     =>
        // create additional patterns with trailing optional arguments omitted
        val omittable = defaultPatterns.reverse.takeWhile(_ accepts TNone).length
        val patterns = for (omitted <- omittable to 0 by -1) yield {
          val pattern = ps.dropRight(omitted) match {
            case Seq()  => TNone
            case Seq(p) => p
            case ps     => PCluster(ps: _*)
          }
          (pattern, ps.length - omitted, omitted)
        }
        // ensure that patterns with omitted arguments are unambiguous
        // ambiguity can occur if the first argument accepts a cluster
        // that matches another pattern with multiple arguments
        for {
          (p1, 1, _) <- patterns
          (pn, n, _) <- patterns
          if n > 1
          p <- pn.expand
        } require(!p1.accepts(p), s"ambiguous patterns: $p accepted with either 1 or $n arguments")
        patterns
    }
    val acceptPat = Pattern.reduce(acceptPats.map(_._1): _*)

    // get return pattern and function to repack the return value into data
    val (returnPat, repack) = inferReturnType(returnType, m.annotations)

    val binder = (self: Any) => {
      // bind default methods to this instance
      val defaultFuncs = defaults.map { _.map(method => invoke(self, method)) }

      // modify unpackers to call default methods if they receive empty data
      val defaultUnpackers = for ((unpack, defaultOpt) <- unpackers zip defaultFuncs) yield {
        defaultOpt match {
          case Some(default) => (d: Data) => if (d.isNone) default(Nil) else unpack(d)
          case None => unpack
        }
      }

      // create a function to unpack incoming data to the appropriate number of parameters,
      // and optionally include the RequestContext as a first argument
      val unpack = (defaultUnpackers, includeRequestContext) match {
        case (Seq(), true)   => (r: RequestContext) => Seq(r)
        case (Seq(), false)  => (r: RequestContext) => Seq()
        case (Seq(f), true)  => (r: RequestContext) => Seq(r, f(r.data))
        case (Seq(f), false) => (r: RequestContext) => Seq(f(r.data))
        case (fs, rc)        => (r: RequestContext) => {
          // extract data into args, adding empty for omitted args based on matching pattern
          val xs = acceptPats.find {
            case (p, _, _) => p.accepts(r.data.t)
          }.map {
            case (p, 0, k) => Seq.fill(k)(Data.NONE)
            case (p, 1, k) => Seq(r.data) ++ Seq.fill(k)(Data.NONE)
            case (p, n, k) => r.data.clusterIterator.toSeq ++ Seq.fill(k)(Data.NONE)
          }.getOrElse {
            sys.error(s"data of type ${r.data.t} not accepted")
          }
          val unpacked = (fs zip xs).map { case (f, x) => f(x) }
          if (rc) {
            r +: unpacked
          } else {
            unpacked
          }
        }
      }
      guard(acceptPat, unpack andThen invoke(self, m) andThen repack)
    }

    (acceptPat, returnPat, binder)
  }

  /** infer the pattern and create an unpacker for a method parameter */
  def inferParamType(tpe: Type, annots: Seq[Annotation]): (Pattern, Data => Any) = {
    val annotated = for {
      a <- annots.find(_.tree.tpe =:= typeOf[org.labrad.annotations.Accept])
      t <- a.param("value").map { case Constant(t: String) => t }
    } yield Pattern(t)
    (patternFor(tpe.dealias, annotated), unpacker(tpe.dealias, annotated))
  }

  /** infer the pattern and create a repacker for a method return type */
  def inferReturnType(tpe: Type, annots: Seq[Annotation]): (Pattern, Any => Data) = {
    val annotated = for {
      a <- annots.find(_.tree.tpe =:= typeOf[org.labrad.annotations.Return])
      t <- a.param("value").map { case Constant(t: String) => t }
    } yield Pattern(t)
    (patternFor(tpe.dealias, annotated), packer(tpe.dealias, annotated))
  }

  implicit class RichAnnotation(a: Annotation) {
    // extract a single parameter by name
    def param(name: String): Option[Constant] = {
      a.tree.children.tail.collectFirst {
        case AssignOrNamedArg(Ident(TermName(`name`)), Literal(c)) => c
      }
    }
  }

  /** create a function that will invoke the given method on the given object */
  private def invoke(self: Any, method: MethodSymbol): Seq[Any] => Any = {
    val func = currentMirror.reflect(self).reflectMethod(method)
    (args: Seq[Any]) =>
      try {
        func(args: _*)
      } catch {
        case e: InvocationTargetException => throw e.getCause
      }
  }

  /** turn a function into a partial function based on a pattern of accepted types */
  private def guard(pattern: Pattern, func: RequestContext => Data): PartialFunction[RequestContext, Data] =
    new PartialFunction[RequestContext, Data] {
      def isDefinedAt(r: RequestContext) = pattern.accepts(r.data.t)
      def apply(r: RequestContext) = func(r)
    }

  def inferType(tpe: Type): (Pattern, Data => Any, PartialFunction[Any, Data]) = (patternFor(tpe), unpacker(tpe), packer(tpe))

  def patternFor[T: TypeTag]: Pattern = patternFor(typeOf[T])

  def patternFor(tpe: Type, pat: Option[Pattern] = None): Pattern = tpe match {
    case tpe if tpe =:= typeOf[Unit]    => Pattern("_")
    case tpe if tpe =:= typeOf[Boolean] => Pattern("b")
    case tpe if tpe =:= typeOf[Int]     => Pattern("i")
    case tpe if tpe =:= typeOf[Long]    => Pattern("w")
    case tpe if tpe =:= typeOf[Double]  =>
      pat match {
        case None => Pattern("v")
        case Some(p: PValue) => p
        case Some(p) => sys.error(s"cannot infer type for $tpe with pattern $p")
      }

    case tpe if tpe =:= typeOf[Date]    => Pattern("t")
    case tpe if tpe =:= typeOf[String]  => Pattern("s")
    case tpe if tpe =:= typeOf[Data]    => Pattern("?")

    case TypeRef(_, Sym("Array") | Sym("Seq"), Seq(t)) => if (t =:= typeOf[Byte]) Pattern("s") else PArr(patternFor(t))

    case TypeRef(_, Sym("Option"), Seq(t)) => Pattern.reduce(PChoice(patternFor(t), Pattern("_")))

    case TypeRef(_, Sym("Either"), Seq(aL, aR)) => PChoice(patternFor(aL), patternFor(aR))

    case TypeRef(_, Sym("Tuple1"), Seq(a1)) => PCluster(patternFor(a1))
    case TypeRef(_, Sym("Tuple2"), Seq(a1, a2)) => PCluster(patternFor(a1), patternFor(a2))
    case TypeRef(_, Sym("Tuple3"), Seq(a1, a2, a3)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3))
    case TypeRef(_, Sym("Tuple4"), Seq(a1, a2, a3, a4)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4))
    case TypeRef(_, Sym("Tuple5"), Seq(a1, a2, a3, a4, a5)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5))
    case TypeRef(_, Sym("Tuple6"), Seq(a1, a2, a3, a4, a5, a6)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6))
    case TypeRef(_, Sym("Tuple7"), Seq(a1, a2, a3, a4, a5, a6, a7)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6), patternFor(a7))
    case TypeRef(_, Sym("Tuple8"), Seq(a1, a2, a3, a4, a5, a6, a7, a8)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6), patternFor(a7), patternFor(a8))
    case TypeRef(_, Sym("Tuple9"), Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6), patternFor(a7), patternFor(a8), patternFor(a9))
    case TypeRef(_, Sym("Tuple10"), Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6), patternFor(a7), patternFor(a8), patternFor(a9), patternFor(a10))
  }

  def unpacker[T: TypeTag]: Data => T = unpacker(typeOf[T]).asInstanceOf[Data => T]

  def unpacker(tpe: Type, pat: Option[Pattern] = None): Data => Any = tpe match {
    case tpe if tpe =:= typeOf[Unit]    => (d: Data) => ()
    case tpe if tpe =:= typeOf[Boolean] => (d: Data) => d.getBool
    case tpe if tpe =:= typeOf[Int]     => (d: Data) => d.getInt
    case tpe if tpe =:= typeOf[Long]    => (d: Data) => d.getUInt
    case tpe if tpe =:= typeOf[Double]  => (d: Data) => d.getValue
    case tpe if tpe =:= typeOf[Date]    => (d: Data) => d.getTime.toDate
    case tpe if tpe =:= typeOf[String]  => (d: Data) => d.getString
    case tpe if tpe =:= typeOf[Data]    => (d: Data) => d

    case TypeRef(_, Sym("Array"), Seq(t)) =>
      t match {
        case t if t =:= typeOf[Byte]    => (d: Data) => d.getBytes
        case t if t =:= typeOf[Boolean] => (d: Data) => d.get[Array[Boolean]]
        case t if t =:= typeOf[Int]     => (d: Data) => d.get[Array[Int]]
        case t if t =:= typeOf[Long]    => (d: Data) => d.get[Array[Long]]
        case t if t =:= typeOf[Double]  => (d: Data) => d.get[Array[Double]]
        case t if t =:= typeOf[String]  => (d: Data) => d.get[Array[String]]
        case t if t =:= typeOf[Data]    => (d: Data) => d.get[Array[Data]]
        case _ =>
          val f = unpacker(t)
          (data: Data) => data.flatIterator.toArray
      }

    case TypeRef(_, Sym("Seq"), Seq(t)) =>
      t match {
        case t if t =:= typeOf[Byte]    => (d: Data) => d.getBytes.toSeq
        case t if t =:= typeOf[Boolean] => (d: Data) => d.get[Seq[Boolean]]
        case t if t =:= typeOf[Int]     => (d: Data) => d.get[Seq[Int]]
        case t if t =:= typeOf[Long]    => (d: Data) => d.get[Seq[Long]]
        case t if t =:= typeOf[String]  => (d: Data) => d.get[Seq[String]]
        case t if t =:= typeOf[Data]    => (d: Data) => d.get[Seq[Data]]
        case _ =>
          val f = unpacker(t)
          (data: Data) => data.flatIterator.toSeq
      }

    case TypeRef(_, Sym("Option"), Seq(t)) =>
      val f = unpacker(t)
      (data: Data) => if (data.isNone) None else Some(f(data))

    case TypeRef(_, Sym("Either"), Seq(aL, aR)) =>
      val (pL, fL, fR) = (patternFor(aL), unpacker(aL), unpacker(aR))
      (data: Data) => if (pL accepts (data.t)) Left(fL(data)) else Right(fR(data))

    case TypeRef(_, Sym("Tuple1"), Seq(a1)) =>
      val f1 = unpacker(a1)
      (_: Data) match { case Cluster(d1) => Tuple1(f1(d1)) }

    case TypeRef(_, Sym("Tuple2"), Seq(a1, a2)) =>
      val (f1, f2) = (unpacker(a1), unpacker(a2))
      (_: Data) match { case Cluster(d1, d2) => (f1(d1), f2(d2)) }

    case TypeRef(_, Sym("Tuple3"), Seq(a1, a2, a3)) =>
      val (f1, f2, f3) = (unpacker(a1), unpacker(a2), unpacker(a3))
      (_: Data) match { case Cluster(d1, d2, d3) => (f1(d1), f2(d2), f3(d3)) }

    case TypeRef(_, Sym("Tuple4"), Seq(a1, a2, a3, a4)) =>
      val (f1, f2, f3, f4) = (unpacker(a1), unpacker(a2), unpacker(a3), unpacker(a4))
      (_: Data) match { case Cluster(d1, d2, d3, d4) => (f1(d1), f2(d2), f3(d3), f4(d4)) }

    case TypeRef(_, Sym("Tuple5"), Seq(a1, a2, a3, a4, a5)) =>
      val (f1, f2, f3, f4, f5) = (unpacker(a1), unpacker(a2), unpacker(a3), unpacker(a4), unpacker(a5))
      (_: Data) match { case Cluster(d1, d2, d3, d4, d5) => (f1(d1), f2(d2), f3(d3), f4(d4), f5(d5)) }

    case TypeRef(_, Sym("Tuple6"), Seq(a1, a2, a3, a4, a5, a6)) =>
      val (f1, f2, f3, f4, f5, f6) = (unpacker(a1), unpacker(a2), unpacker(a3), unpacker(a4), unpacker(a5), unpacker(a6))
      (_: Data) match { case Cluster(d1, d2, d3, d4, d5, d6) => (f1(d1), f2(d2), f3(d3), f4(d4), f5(d5), f6(d6)) }

    case TypeRef(_, Sym("Tuple7"), Seq(a1, a2, a3, a4, a5, a6, a7)) =>
      val (f1, f2, f3, f4, f5, f6, f7) = (unpacker(a1), unpacker(a2), unpacker(a3), unpacker(a4), unpacker(a5), unpacker(a6), unpacker(a7))
      (_: Data) match { case Cluster(d1, d2, d3, d4, d5, d6, d7) => (f1(d1), f2(d2), f3(d3), f4(d4), f5(d5), f6(d6), f7(d7)) }

    case TypeRef(_, Sym("Tuple8"), Seq(a1, a2, a3, a4, a5, a6, a7, a8)) =>
      val (f1, f2, f3, f4, f5, f6, f7, f8) = (unpacker(a1), unpacker(a2), unpacker(a3), unpacker(a4), unpacker(a5), unpacker(a6), unpacker(a7), unpacker(a8))
      (_: Data) match { case Cluster(d1, d2, d3, d4, d5, d6, d7, d8) => (f1(d1), f2(d2), f3(d3), f4(d4), f5(d5), f6(d6), f7(d7), f8(d8)) }

    case TypeRef(_, Sym("Tuple9"), Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9)) =>
      val (f1, f2, f3, f4, f5, f6, f7, f8, f9) = (unpacker(a1), unpacker(a2), unpacker(a3), unpacker(a4), unpacker(a5), unpacker(a6), unpacker(a7), unpacker(a8), unpacker(a9))
      (_: Data) match { case Cluster(d1, d2, d3, d4, d5, d6, d7, d8, d9) => (f1(d1), f2(d2), f3(d3), f4(d4), f5(d5), f6(d6), f7(d7), f8(d8), f9(d9)) }

    case TypeRef(_, Sym("Tuple10"), Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)) =>
      val (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10) = (unpacker(a1), unpacker(a2), unpacker(a3), unpacker(a4), unpacker(a5), unpacker(a6), unpacker(a7), unpacker(a8), unpacker(a9), unpacker(a10))
      (_: Data) match { case Cluster(d1, d2, d3, d4, d5, d6, d7, d8, d9, d10) => (f1(d1), f2(d2), f3(d3), f4(d4), f5(d5), f6(d6), f7(d7), f8(d8), f9(d9), f10(d10)) }
  }

  def toData[T: TypeTag](value: T): Data = { val f = packer[T]; f(value) }

  def packer[T: TypeTag]: PartialFunction[Any, Data] = packer(typeOf[T])

  def packer(tpe: Type, pat: Option[Pattern] = None): PartialFunction[Any, Data] = tpe match {
    case tpe if tpe =:= typeOf[Unit]    => { case _ => Data.NONE }
    case tpe if tpe =:= typeOf[Boolean] => { case x: Boolean => Bool(x) }
    case tpe if tpe =:= typeOf[Int]     => { case x: Int => Integer(x) }
    case tpe if tpe =:= typeOf[Long]    => { case x: Long => UInt(x) }
    case tpe if tpe =:= typeOf[Double]  =>
      pat match {
        case None => { case x: Double => Value(x) }
        case Some(PValue(units)) => { case x: Double => Value(x, units) }
        case Some(p) => sys.error(s"cannot create packer for type $tpe with pattern $p")
      }

    case tpe if tpe =:= typeOf[Date]    => { case x: Date => val d = Data("t"); d.setTime(x); d }
    case tpe if tpe =:= typeOf[String]  => { case x: String => Str(x) }
    case tpe if tpe =:= typeOf[Data]    => { case x: Data => x }

    case TypeRef(_, Sym("Array"), Seq(t)) =>
      t match {
        case t if t =:= typeOf[Byte]    => { case x: Array[Byte] => Bytes(x) }
        case t if t =:= typeOf[Boolean] => { case x: Array[Boolean] => Arr(x) }
        case t if t =:= typeOf[Int]     => { case x: Array[Int] => Arr(x) }
        case t if t =:= typeOf[Long]    => { case x: Array[Long] => Arr(x) }
        case t if t =:= typeOf[Double]  => { case x: Array[Double] => Arr(x) }
        case t if t =:= typeOf[String]  => { case x: Array[String] => Arr(x) }
        case t if t =:= typeOf[Data]    => { case x: Array[Data] => Arr(x) }
        case _ =>
          val f: Any => Data = packer(t)
          (_: Any) match { case x: Array[_] => Arr(x map f) }
      }

    case TypeRef(_, Sym("Seq"), Seq(t)) =>
      t match {
        case t if t =:= typeOf[Byte]    => { case x: Seq[_] => Bytes(x.asInstanceOf[Seq[Byte]].toArray) }
        case t if t =:= typeOf[Boolean] => { case x: Seq[_] => Arr(x.asInstanceOf[Seq[Boolean]].toArray) }
        case t if t =:= typeOf[Int]     => { case x: Seq[_] => Arr(x.asInstanceOf[Seq[Int]].toArray) }
        case t if t =:= typeOf[Long]    => { case x: Seq[_] => Arr(x.asInstanceOf[Seq[Long]].toArray) }
        case t if t =:= typeOf[String]  => { case x: Seq[_] => Arr(x.asInstanceOf[Seq[String]].toArray) }
        case t if t =:= typeOf[Data]    => { case x: Seq[_] => Arr(x.asInstanceOf[Seq[Data]].toArray) }
        case _ =>
          val f = packer(t)
          (_: Any) match { case x: Seq[_] => Arr(x map f) }
      }

    case TypeRef(_, Sym("Option"), Seq(t)) =>
      val f = packer(t)
      (_: Any) match { case Some(x) => f(x); case None => Data.NONE }

    case TypeRef(_, Sym("Either"), Seq(aL, aR)) =>
      val (fL, fR) = (packer(aL), packer(aR))
      (_: Any) match { case Left(x) => fL(x); case Right(y) => fR(y) }

    case TypeRef(_, Sym("Tuple1"), Seq(a1)) =>
      val f1 = packer(a1)
      (_: Any) match { case Tuple1(x) => Cluster(f1(x)) }

    case TypeRef(_, Sym("Tuple2"), Seq(a1, a2)) =>
      val (f1, f2) = (packer(a1), packer(a2))
      (_: Any) match { case (x1, x2) => Cluster(f1(x1), f2(x2)) }

    case TypeRef(_, Sym("Tuple3"), Seq(a1, a2, a3)) =>
      val (f1, f2, f3) = (packer(a1), packer(a2), packer(a3))
      (_: Any) match { case (x1, x2, x3) => Cluster(f1(x1), f2(x2), f3(x3)) }

    case TypeRef(_, Sym("Tuple4"), Seq(a1, a2, a3, a4)) =>
      val (f1, f2, f3, f4) = (packer(a1), packer(a2), packer(a3), packer(a4))
      (_: Any) match { case (x1, x2, x3, x4) => Cluster(f1(x1), f2(x2), f3(x3), f4(x4)) }

    case TypeRef(_, Sym("Tuple5"), Seq(a1, a2, a3, a4, a5)) =>
      val (f1, f2, f3, f4, f5) = (packer(a1), packer(a2), packer(a3), packer(a4), packer(a5))
      (_: Any) match { case (x1, x2, x3, x4, x5) => Cluster(f1(x1), f2(x2), f3(x3), f4(x4), f5(x5)) }

    case TypeRef(_, Sym("Tuple6"), Seq(a1, a2, a3, a4, a5, a6)) =>
      val (f1, f2, f3, f4, f5, f6) = (packer(a1), packer(a2), packer(a3), packer(a4), packer(a5), packer(a6))
      (_: Any) match { case (x1, x2, x3, x4, x5, x6) => Cluster(f1(x1), f2(x2), f3(x3), f4(x4), f5(x5), f6(x6)) }

    case TypeRef(_, Sym("Tuple7"), Seq(a1, a2, a3, a4, a5, a6, a7)) =>
      val (f1, f2, f3, f4, f5, f6, f7) = (packer(a1), packer(a2), packer(a3), packer(a4), packer(a5), packer(a6), packer(a7))
      (_: Any) match { case (x1, x2, x3, x4, x5, x6, x7) => Cluster(f1(x1), f2(x2), f3(x3), f4(x4), f5(x5), f6(x6), f7(x7)) }

    case TypeRef(_, Sym("Tuple8"), Seq(a1, a2, a3, a4, a5, a6, a7, a8)) =>
      val (f1, f2, f3, f4, f5, f6, f7, f8) = (packer(a1), packer(a2), packer(a3), packer(a4), packer(a5), packer(a6), packer(a7), packer(a8))
      (_: Any) match { case (x1, x2, x3, x4, x5, x6, x7, x8) => Cluster(f1(x1), f2(x2), f3(x3), f4(x4), f5(x5), f6(x6), f7(x7), f8(x8)) }

    case TypeRef(_, Sym("Tuple9"), Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9)) =>
      val (f1, f2, f3, f4, f5, f6, f7, f8, f9) = (packer(a1), packer(a2), packer(a3), packer(a4), packer(a5), packer(a6), packer(a7), packer(a8), packer(a9))
      (_: Any) match { case (x1, x2, x3, x4, x5, x6, x7, x8, x9) => Cluster(f1(x1), f2(x2), f3(x3), f4(x4), f5(x5), f6(x6), f7(x7), f8(x8), f9(x9)) }

    case TypeRef(_, Sym("Tuple10"), Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)) =>
      val (f1, f2, f3, f4, f5, f6, f7, f8, f9, f10) = (packer(a1), packer(a2), packer(a3), packer(a4), packer(a5), packer(a6), packer(a7), packer(a8), packer(a9), packer(a10))
      (_: Any) match { case (x1, x2, x3, x4, x5, x6, x7, x8, x9, x10) => Cluster(f1(x1), f2(x2), f3(x3), f4(x4), f5(x5), f6(x6), f7(x7), f8(x8), f9(x9), f10(x10)) }
  }

  object Sym {
    def unapply(sym: Symbol): Option[String] = Some(sym.name.decodedName.toString)
  }

  def arg0(): String = ""
  def arg1(a: String): String = ""
  def arg2(a: String, b: String): String = ""

  val handlers = mutable.Map.empty[Long, Data => Data]

  def _handle(id: Long, name: String, doc: String, handler: Data => Data): Unit = {
    handlers(id) = handler
  }
  def handle[B: TypeTag](id: Long, name: String, doc: String)(f: () => B): Unit = _handle(id, name, doc, lift(f))
  def handle[A: TypeTag, B: TypeTag](id: Long, name: String, doc: String)(f: (A) => B): Unit = _handle(id, name, doc, lift(f))
  def handle[A1: TypeTag, A2: TypeTag, B: TypeTag](id: Long, name: String, doc: String)(f: (A1, A2) => B): Unit = _handle(id, name, doc, lift(f))
  def handle[A1: TypeTag, A2: TypeTag, A3: TypeTag, B: TypeTag](id: Long, name: String, doc: String)(f: (A1, A2, A3) => B): Unit = _handle(id, name, doc, lift(f))
  def handle[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, B: TypeTag](id: Long, name: String, doc: String)(f: (A1, A2, A3, A4) => B): Unit = _handle(id, name, doc, lift(f))
  def handle[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, B: TypeTag](id: Long, name: String, doc: String)(f: (A1, A2, A3, A4, A5) => B): Unit = _handle(id, name, doc, lift(f))
  def handle[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, B: TypeTag](id: Long, name: String, doc: String)(f: (A1, A2, A3, A4, A5, A6) => B): Unit = _handle(id, name, doc, lift(f))
  def handle[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, B: TypeTag](id: Long, name: String, doc: String)(f: (A1, A2, A3, A4, A5, A6, A7) => B): Unit = _handle(id, name, doc, lift(f))
  def handle[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, B: TypeTag](id: Long, name: String, doc: String)(f: (A1, A2, A3, A4, A5, A6, A7, A8) => B): Unit = _handle(id, name, doc, lift(f))
  def handle[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, B: TypeTag](id: Long, name: String, doc: String)(f: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => B): Unit = _handle(id, name, doc, lift(f))

  def handleS[A1: TypeTag, A2: TypeTag, B: TypeTag](f: (A1, Seq[A2]) => B): Unit = {}
  def f(a1: Int, a2: Int*): String = a2.mkString(",")
  handleS(f)

  // for later...
  def lift[B: TypeTag](f: () => B): Data => Data =
    ((_: Data) => f()) andThen packer[B]

  def lift[A: TypeTag, B: TypeTag](f: A => B): Data => Data =
    unpacker[A] andThen f andThen packer[B]

  def lift[A1: TypeTag, A2: TypeTag, B: TypeTag](f: (A1, A2) => B): Data => Data =
    unpacker[(A1, A2)] andThen f.tupled andThen packer[B]

  def lift[A1: TypeTag, A2: TypeTag, A3: TypeTag, B: TypeTag](f: (A1, A2, A3) => B): Data => Data =
    unpacker[(A1, A2, A3)] andThen f.tupled andThen packer[B]

  def lift[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, B: TypeTag](f: (A1, A2, A3, A4) => B): Data => Data =
    unpacker[(A1, A2, A3, A4)] andThen f.tupled andThen packer[B]

  def lift[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, B: TypeTag](f: (A1, A2, A3, A4, A5) => B): Data => Data =
    unpacker[(A1, A2, A3, A4, A5)] andThen f.tupled andThen packer[B]

  def lift[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, B: TypeTag](f: (A1, A2, A3, A4, A5, A6) => B): Data => Data =
    unpacker[(A1, A2, A3, A4, A5, A6)] andThen f.tupled andThen packer[B]

  def lift[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, B: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7) => B): Data => Data =
    unpacker[(A1, A2, A3, A4, A5, A6, A7)] andThen f.tupled andThen packer[B]

  def lift[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, B: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7, A8) => B): Data => Data =
    unpacker[(A1, A2, A3, A4, A5, A6, A7, A8)] andThen f.tupled andThen packer[B]

  def lift[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, B: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => B): Data => Data =
    unpacker[(A1, A2, A3, A4, A5, A6, A7, A8, A9)] andThen f.tupled andThen packer[B]

  def liftAsync[A: TypeTag, B: TypeTag](f: A => Future[B]) = {
    val unpack = unpacker[A]
    val pack = packer[B]
    new FutureFunction1[Data, Data] {
      def apply(data: Data)(implicit ec: ExecutionContext): Future[Data] =
        f(unpack(data)).map(pack)
    }
  }

  def liftAsync[A1: TypeTag, A2: TypeTag, B: TypeTag](f: (A1, A2) => Future[B]) = {
    val unpack = unpacker[(A1, A2)]
    val pack = packer[B]
    val ft = f.tupled
    new FutureFunction1[Data, Data] {
      def apply(data: Data)(implicit ec: ExecutionContext): Future[Data] =
        ft(unpack(data)).map(pack)
    }
  }

  def liftAsync[A1: TypeTag, A2: TypeTag, A3: TypeTag, B: TypeTag](f: (A1, A2, A3) => Future[B]) = {
    val unpack = unpacker[(A1, A2, A3)]
    val pack = packer[B]
    val ft = f.tupled
    new FutureFunction1[Data, Data] {
      def apply(data: Data)(implicit ec: ExecutionContext): Future[Data] =
        ft(unpack(data)).map(pack)
    }
  }

  def liftAsync[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, B: TypeTag](f: (A1, A2, A3, A4) => Future[B]) = {
    val unpack = unpacker[(A1, A2, A3, A4)]
    val pack = packer[B]
    val ft = f.tupled
    new FutureFunction1[Data, Data] {
      def apply(data: Data)(implicit ec: ExecutionContext): Future[Data] =
        ft(unpack(data)).map(pack)
    }
  }

  def liftAsync[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, B: TypeTag](f: (A1, A2, A3, A4, A5) => Future[B]) = {
    val unpack = unpacker[(A1, A2, A3, A4, A5)]
    val pack = packer[B]
    val ft = f.tupled
    new FutureFunction1[Data, Data] {
      def apply(data: Data)(implicit ec: ExecutionContext): Future[Data] =
        ft(unpack(data)).map(pack)
    }
  }

  def liftAsync[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, B: TypeTag](f: (A1, A2, A3, A4, A5, A6) => Future[B]) = {
    val unpack = unpacker[(A1, A2, A3, A4, A5, A6)]
    val pack = packer[B]
    val ft = f.tupled
    new FutureFunction1[Data, Data] {
      def apply(data: Data)(implicit ec: ExecutionContext): Future[Data] =
        ft(unpack(data)).map(pack)
    }
  }

  def liftAsync[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, B: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7) => Future[B]) = {
    val unpack = unpacker[(A1, A2, A3, A4, A5, A6, A7)]
    val pack = packer[B]
    val ft = f.tupled
    new FutureFunction1[Data, Data] {
      def apply(data: Data)(implicit ec: ExecutionContext): Future[Data] =
        ft(unpack(data)).map(pack)
    }
  }

  def liftAsync[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, B: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7, A8) => Future[B]) = {
    val unpack = unpacker[(A1, A2, A3, A4, A5, A6, A7, A8)]
    val pack = packer[B]
    val ft = f.tupled
    new FutureFunction1[Data, Data] {
      def apply(data: Data)(implicit ec: ExecutionContext): Future[Data] =
        ft(unpack(data)).map(pack)
    }
  }

  def liftAsync[A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, B: TypeTag](f: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => Future[B]) = {
    val unpack = unpacker[(A1, A2, A3, A4, A5, A6, A7, A8, A9)]
    val pack = packer[B]
    val ft = f.tupled
    new FutureFunction1[Data, Data] {
      def apply(data: Data)(implicit ec: ExecutionContext): Future[Data] =
        ft(unpack(data)).map(pack)
    }
  }

  trait FutureFunction1[-T1, +R] {
    def apply(data: T1)(implicit ec: ExecutionContext): Future[R]
  }
}
