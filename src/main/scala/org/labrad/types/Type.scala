package org.labrad.types

import scala.collection.mutable
import scala.util.parsing.combinator.RegexParsers

object Parsers extends RegexParsers {

  private def stripComments(tag: String): String =
    tag.replaceAll("""\{[^\{\}]*\}""", "") // remove bracketed comments
       .split(":")(0) // strip off any trailing comments

  private def parseOrThrow[A](p: Parser[A], s: String): A =
    parseAll(p, s) match {
      case Success(x, _) => x
      case NoSuccess(msg, _) => sys.error(msg)
    }

  def parsePattern(tag: String): Pattern = parseOrThrow(aPattern, stripComments(tag))
  def parseType(tag: String): Type = parseOrThrow(aType, stripComments(tag))
  def parseUnit(s: String): Seq[(String, Ratio)] = parseOrThrow(unitStr, s)


  def aType: Parser[Type] =
    ( errorType
    | repsep(singleType, ",".?) ^^ {
        case Seq()  => TNone
        case Seq(t) => t
        case ts     => TCluster(ts: _*)
      }
    )

  def noneType: Parser[Type] =
      "_" ^^ { _ => TNone }

  def errorType: Parser[Type] =
      "E" ~> singleType.? ^^ { t => TError(t getOrElse TNone) }

  def someType: Parser[Type] =
    ( "b"   ^^ { _ => TBool }
    | "i"   ^^ { _ => TInt }
//    | "i8"  ^^ { _ => TInt8 }
//    | "i16" ^^ { _ => TInt16 }
//    | "i32" ^^ { _ => TInt }
//    | "i64" ^^ { _ => TInt64 }
    | "w"   ^^ { _ => TUInt }
//    | "u8"  ^^ { _ => TUInt8 }
//    | "u16" ^^ { _ => TUInt16 }
//    | "u32" ^^ { _ => TUInt }
//    | "u64" ^^ { _ => TUInt64 }
    | "s"   ^^ { _ => TStr }
    | "t"   ^^ { _ => TTime }
    | valueType
    | complexType
    | arrayType
    | clusterType
    )

  def singleType: Parser[Type] = noneType | someType

  def valueType: Parser[Type] =
      "v" ~> units.? ^^ { u => TValue(u) }

  def complexType: Parser[Type] =
      "c" ~> units.? ^^ { u => TComplex(u) }

  def units: Parser[String] = "[" ~> """[^\[\]]*""".r <~ "]"

  def arrayType: Parser[Type] =
      "*" ~> number.? ~ singleType ^^ { case d ~ t => TArr(t, d getOrElse 1) }

  def number: Parser[Int] = """\d+""".r ^^ { _.toInt }

  def clusterType: Parser[Type] =
      "(" ~> repsep(singleType, ",".?) <~ ")" ^^ { ts => TCluster(ts: _*) }



  def aPattern: Parser[Pattern] =
    ( errorPattern
    | repsep(nonemptyPattern, "|") ^^ {
        case Seq()  => TNone
        case Seq(p) => p
        case ps     => PChoice(ps: _*)
      }
    )

  def nonemptyPattern: Parser[Pattern] =
      rep1sep(singlePattern, ",".?) ^^ {
        case Seq(p) => p
        case ps     => PCluster(ps: _*)
      }

  def errorPattern: Parser[Pattern] =
      "E" ~> singlePattern.? ^^ { p => PError(p getOrElse TNone) }

  def somePattern: Parser[Pattern] =
    ( "?" ^^ { _ => PAny }
    | valuePattern
    | complexPattern
    | arrayPattern
    | expandoPattern
    | clusterPattern
    | choicePattern
    | someType
    )

  def singlePattern: Parser[Pattern] = noneType | somePattern

  def valuePattern: Parser[Pattern] =
      "v" ~> units.? ^^ { u => PValue(u) }

  def complexPattern: Parser[Pattern] =
      "c" ~> units.? ^^ { u => PComplex(u) }

  def arrayPattern: Parser[Pattern] =
      "*" ~> number.? ~ singlePattern ^^ { case d ~ t => PArr(t, d getOrElse 1) }

  def expandoPattern: Parser[Pattern] =
      "(" ~> somePattern <~ "...)" ^^ { p => PExpando(p) }

  def clusterPattern: Parser[Pattern] =
      "(" ~> repsep(somePattern, ",".?) <~ ")" ^^ { ps => PCluster(ps: _*) }

  def choicePattern: Parser[Pattern] =
      "<" ~> repsep(nonemptyPattern, "|") <~ ">" ^^ { ps => PChoice(ps: _*) }


  def unitStr: Parser[Seq[(String, Ratio)]] =
      firstTerm ~ (divTerm | mulTerm).* ^^ {
        case first ~ rest => first :: rest
      }

  def firstTerm = "1".? ~> divTerm | term

  def mulTerm = "*" ~> term
  def divTerm = "/" ~> term ^^ { case (name, exp) => (name, -exp) }

  def term: Parser[(String, Ratio)] =
      termName ~ exponent.? ^^ {
        case name ~ None      => (name, Ratio(1))
        case name ~ Some(exp) => (name, exp)
      }

  def termName = """[A-Za-z'"]+""".r

  def exponent: Parser[Ratio] =
      "^" ~> "-".? ~ number ~ ("/" ~> number).? ^^ {
        case None    ~ n ~ None    => Ratio( n)
        case None    ~ n ~ Some(d) => Ratio( n, d)
        case Some(_) ~ n ~ None    => Ratio(-n)
        case Some(_) ~ n ~ Some(d) => Ratio(-n, d)
      }
}


sealed trait Pattern extends Serializable { self: Pattern =>
  def accepts(tag: String): Boolean = accepts(Type(tag))
  def accepts(pat: Pattern): Boolean

  def apply(typ: Type): Option[Type]

  private def combinations[T](seqs: Seq[Seq[T]]): Seq[Seq[T]] = seqs match {
    case Seq() => Seq(Seq())
    case Seq(heads, rest @ _*) =>
      val tails = combinations(rest)
      for (head <- heads; tail <- tails) yield head +: tail
  }

  // TODO: remove pattern expansion once clients support patterns
  def expand: Seq[Pattern] = self match {
    case PChoice(ps @ _*) => ps.flatMap(_.expand)
    case PCluster(ps @ _*) => combinations(ps.map(_.expand)).map(PCluster(_: _*))
    case PExpando(p) => Seq(PAny)
    case PArr(p, depth) => p.expand.map(PArr(_, depth))
    case p => Seq(p)
  }
}


object Pattern {
  def apply(s: String): Pattern = Parsers.parsePattern(s)

  def reduce(pats: Pattern*): Pattern = {
    def flattenChoices(p: Pattern): Seq[Pattern] = p match {
      case PChoice(ps @ _*) => ps.flatMap(flattenChoices)
      case p => Seq(p)
    }

    val pat = PChoice(pats: _*)
    val buf = mutable.Buffer.empty[Pattern]
    for (p <- flattenChoices(pat)) if (!buf.exists(_ accepts p)) buf += p
    buf.toSeq match {
      case Seq(p) => p
      case ps => PChoice(ps: _*)
    }
  }

  /**
   * Determine whether a given pattern accepts another pattern.
   *
   * This handles correctly the case where either pattern is a PChoice, which
   * the accepts methods on individual Pattern subclasses do not.
   * TODO: refactor accepts methods on Pattern subclasses to use this, so they
   * will work against all patterns, including PChoice.
   */
  def accepts(a: Pattern, b: Pattern): Boolean = {
    (a, b) match {
      case (PChoice(as @ _*), PChoice(bs @ _*)) =>
        bs.forall(b => as.exists(a => a.accepts(b)))

      case (PChoice(as @ _*), b) =>
        as.exists(a => a.accepts(b))

      case (a, PChoice(bs @ _*)) =>
        bs.forall(b => a.accepts(b))

      case (a, b) =>
        a.accepts(b)
    }
  }
}


/** implementation of important Pattern methods for concrete types */
trait ConcreteType { self: Type =>
  def accepts(typ: Pattern) = typ == self

  def apply(typ: Type): Option[Type] =
    if (typ == self) Some(self) else None
}


sealed trait Type extends Pattern {
  /** Indicates whether data of this type has fixed byte length */
  def fixedWidth: Boolean

  /** Byte width for data of this type */
  def dataWidth: Int
}

object Type {
  def apply(s: String): Type = Parsers.parseType(s)
}



case object PAny extends Pattern {
  def accepts(typ: Pattern) = true
  def apply(typ: Type): Option[Type] = Some(typ)

  override def toString = "?"
}


case class PChoice(choices: Pattern*) extends Pattern {
  def accepts(pat: Pattern) = pat match {
    case PChoice(ps @ _*) => ps forall { p => choices.exists(_ accepts p) }
    case p => choices.exists(_ accepts p)
  }

  def apply(typ: Type): Option[Type] = {
    choices.view.map(_(typ)).collectFirst { case Some(t) => t }
  }

  override def toString = s"<${choices.mkString("|")}>"
}


case class PExpando(pat: Pattern) extends Pattern {
  def accepts(typ: Pattern) = typ match {
    case PExpando(p) => pat accepts p
    case PCluster(elems @ _*) => elems forall { pat accepts _ }
    case _ => false
  }

  def apply(typ: Type): Option[Type] = typ match {
    case TCluster(elems @ _*) =>
      val types = for (e <- elems) yield pat(e).getOrElse(return None)
      Some(TCluster(types: _*))
    case _ => None
  }

  override def toString = s"($pat...)"
}


class PCluster protected(val elems: Pattern*) extends Pattern {
  def accepts(typ: Pattern) = typ match {
    case PCluster(others @ _*) if others.size == elems.size =>
      (elems zip others) forall { case (elem, other) => elem accepts other }
    case _ => false
  }

  def apply(typ: Type): Option[Type] = typ match {
    case TCluster(es @ _*) if es.length == elems.length =>
      val types = for ((elem, e) <- elems zip es) yield elem(e).getOrElse(return None)
      Some(TCluster(types: _*))
    case _ => None
  }

  override def toString = s"(${elems.mkString})"

  override def equals(other: Any): Boolean = other match {
    case p: PCluster => p.elems == elems
    case _ => false
  }

  override def hashCode: Int = elems.hashCode
}

object PCluster {
  def apply(elems: Pattern*) = new PCluster(elems: _*)

  def unapplySeq(pat: Pattern): Option[Seq[Pattern]] = pat match {
    case p: PCluster => Some(p.elems)
    case _ => None
  }
}

case class TCluster(override val elems: Type*) extends PCluster(elems: _*) with Type {
  lazy val dataWidth = elems.map(_.dataWidth).sum
  lazy val fixedWidth = elems.forall(_.fixedWidth)
  lazy val offsets = elems.map(_.dataWidth).scan(0)(_ + _).dropRight(1)

  def size = elems.size
  def apply(i: Int) = elems(i)
  def offset(i: Int) = offsets(i)
}


class PArr protected(val elem: Pattern, val depth: Int) extends Pattern {
  require(depth > 0)

  def accepts(typ: Pattern) = typ match {
    case PArr(e, d) if d == depth => elem accepts e
    case _ => false
  }

  def apply(typ: Type): Option[Type] = typ match {
    case TArr(e, d) if d == depth => elem(e).map(TArr(_, d))
    case _ => None
  }

  override val toString = depth match {
    case 1 => "*" + elem.toString
    case d => "*" + d + elem.toString
  }

  override def equals(other: Any): Boolean = other match {
    case p: PArr => p.elem == elem && p.depth == depth
    case _ => false
  }

  override def hashCode: Int = (elem, depth).hashCode
}

object PArr {
  def apply(elem: Pattern, depth: Int = 1) = new PArr(elem, depth)

  def unapply(pat: Pattern): Option[(Pattern, Int)] = pat match {
    case p: PArr => Some((p.elem, p.depth))
    case _ => None
  }
}

case class TArr(override val elem: Type, override val depth: Int = 1) extends PArr(elem, depth) with Type {
  override val toString = depth match {
    case 1 => "*" + elem.toString
    case d => "*" + d + elem.toString
  }

  val fixedWidth = false
  val dataWidth = 4 * depth + 4

  def offset(index: Int) = index * elem.dataWidth
}


class PError protected(val payload: Pattern) extends Pattern {
  def accepts(typ: Pattern) = typ match {
    case PError(p) => payload accepts p
    case _ => false
  }

  def apply(typ: Type): Option[Type] = typ match {
    case TError(p) => payload(p).map(TError(_))
    case _ => None
  }

  override val toString = "E" + payload.toString

  override def equals(other: Any): Boolean = other match {
    case p: PError => p.payload == payload
    case _ => false
  }

  override def hashCode: Int = payload.hashCode
}

object PError {
  def apply(payload: Pattern = TNone) = new PError(payload)

  def unapply(pat: Pattern): Option[Pattern] = pat match {
    case p: PError => Some(p.payload)
    case _ => None
  }
}

case class TError(override val payload: Type) extends PError(payload) with Type {
  val fixedWidth = false
  val dataWidth = 4 + 4 + payload.dataWidth
}


case object TNone extends Type with ConcreteType {
  override val toString = "_"
  val fixedWidth = true
  val dataWidth = 0
}

case object TBool extends Type with ConcreteType {
  override val toString = "b"
  val fixedWidth = true
  val dataWidth = 1
}

case object TInt extends Type with ConcreteType {
  override val toString = "i"
  val fixedWidth = true
  val dataWidth = 4
}

//case object TInt8 extends Type with ConcreteType {
//  override val toString = "i8"
//  val fixedWidth = true
//  val dataWidth = 1
//}
//
//case object TInt16 extends Type with ConcreteType {
//  override val toString = "i16"
//  val fixedWidth = true
//  val dataWidth = 2
//}
//
//case object TInt64 extends Type with ConcreteType {
//  override val toString = "i64"
//  val fixedWidth = true
//  val dataWidth = 8
//}

case object TUInt extends Type with ConcreteType {
  override val toString = "w"
  val fixedWidth = true
  val dataWidth = 4
}

//case object TUInt8 extends Type with ConcreteType {
//  override val toString = "w8"
//  val fixedWidth = true
//  val dataWidth = 1
//}
//
//case object TUInt16 extends Type with ConcreteType {
//  override val toString = "w16"
//  val fixedWidth = true
//  val dataWidth = 2
//}
//
//case object TUInt64 extends Type with ConcreteType {
//  override val toString = "w64"
//  val fixedWidth = true
//  val dataWidth = 8
//}

case object TStr extends Type with ConcreteType {
  override val toString = "s"
  val fixedWidth = false
  val dataWidth = 4
}

case object TTime extends Type with ConcreteType {
  override val toString = "t"
  val fixedWidth = true
  val dataWidth = 16
}


class PValue protected(val units: Option[String]) extends Pattern {
  def accepts(typ: Pattern) = typ match {
    case PValue(u) => (units, u) match {
      case (None, None) => true
      case (None, Some(u)) => true
      case (Some(units), None) => true
      case (Some(units), Some(u)) => units == u
    }
    case _ => false
  }

  def apply(typ: Type): Option[Type] = typ match {
    case TValue(unit) =>
      units match {
        case Some(u) => Some(TValue(u))
        case None => Some(TValue(unit))
      }
    case _ => None
  }

  override val toString = units match {
    case None => "v"
    case Some(units) => s"v[$units]"
  }

  override def equals(other: Any): Boolean = other match {
    case p: PValue => p.units == units
    case _ => false
  }

  override def hashCode: Int = units.hashCode
}

object PValue {
  def apply(units: String) = new PValue(Option(units))
  def apply(units: Option[String] = None) = new PValue(units)

  def unapply(pat: Pattern): Option[Option[String]] = pat match {
    case p: PValue => Some(p.units)
    case _ => None
  }
}

case class TValue(override val units: Option[String] = None) extends PValue(units) with Type {
  val fixedWidth = true
  val dataWidth = 8
}

object TValue {
  def apply(units: String) = new TValue(Option(units))
}


class PComplex protected(val units: Option[String]) extends Pattern {
  def accepts(typ: Pattern) = typ match {
    case PComplex(u) => (units, u) match {
      case (None, None) => true
      case (None, Some(u)) => true
      case (Some(units), None) => true
      case (Some(units), Some(u)) => units == u
    }
    case _ => false
  }

  def apply(typ: Type): Option[Type] = typ match {
    case TComplex(unit) =>
      units match {
        case Some(u) => Some(TComplex(u))
        case None => Some(TComplex(unit))
      }
    case _ => None
  }

  override val toString = units match {
    case None => "c"
    case Some(units) => s"c[$units]"
  }

  override def equals(other: Any): Boolean = other match {
    case p: PComplex => p.units == units
    case _ => false
  }

  override def hashCode: Int = units.hashCode
}

object PComplex {
  def apply(units: String) = new PComplex(Option(units))
  def apply(units: Option[String] = None) = new PComplex(units)

  def unapply(pat: Pattern): Option[Option[String]] = pat match {
    case p: PComplex => Some(p.units)
    case _ => None
  }
}

case class TComplex(override val units: Option[String] = None) extends PComplex(units) with Type {
  def fixedWidth = true
  def dataWidth = 16
}

object TComplex {
  def apply(units: String) = new TComplex(Option(units))
}
