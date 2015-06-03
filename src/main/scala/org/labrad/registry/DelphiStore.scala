package org.labrad.registry

import java.nio.charset.StandardCharsets.UTF_8
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.labrad.data._
import org.labrad.types._
import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers


/**
 * Parsers for the string representation used by the legacy delphi code.
 */
object DelphiParsers extends RegexParsers {

  def parseData(s: String): Data =
    parseAll(data, s) match {
      case Success(d, _) => d
      case NoSuccess(msg, _) => sys.error(msg)
    }

  def data: Parser[Data] =
    ( nonArrayData | array )

  def nonArrayData: Parser[Data] =
    ( none | bool | complex | value | time | int | uint | string | cluster )

  def none: Parser[Data] =
      "_" ^^ { _ => Data.NONE }

  def bool: Parser[Data] =
    ( "[Tt]rue".r ^^ { _ => Bool(true) }
    | "[Ff]alse".r ^^ { _ => Bool(false) }
    )

  def int: Parser[Data] =
    ( "+" ~> unsignedInt ^^ { x => Integer(x.toInt) }
    | "-" ~> unsignedInt ^^ { x => Integer(-x.toInt) }
    )

  def uint: Parser[Data] =
    unsignedInt ^^ { num => UInt(num) }

  def unsignedInt: Parser[Long] =
    """\d+""".r ^^ { _.toLong }

  def string: Parser[Data] =
    """('[^'\x00-\x1F\x7F-\xFF]*'|#[0-9]{1,3})+""".r ^^ { s => Bytes(Translate.stringToBytes(s)) }

  val DelphiFormat = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss.SSS")
  def time: Parser[Data] =
    """\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}(.\d{1,3})?""".r ~ """\d*(E-\d+)?""".r ^^ { case s ~ ignored => Time(DelphiFormat.parseDateTime(s).toDate) }

  def value: Parser[Data] =
      signedReal ~ (' ' ~> units).? ^^ { case num ~ unit => Value(num, unit) }

  def complex: Parser[Data] =
      complexNum ~ (' ' ~> units).? ^^ { case (re, im) ~ unit => Cplx(re, im, unit) }

  def signedReal: Parser[Double] =
    ( "+" ~> unsignedReal ^^ { x => x }
    | "-" ~> unsignedReal ^^ { x => -x }
    | unsignedReal
    )

  def unsignedReal: Parser[Double] =
    ( "NAN.0" ^^ { _ => Double.NaN }
    | "INF.0" ^^ { _ => Double.PositiveInfinity }
    | """(\d*\.\d+|\d+(\.\d*)?)[eE][+-]?\d+""".r ^^ { _.toDouble }
    | """\d*\.\d+""".r ^^ { _.toDouble }
    )

  def complexNum: Parser[(Double, Double)] =
      signedReal ~ ("+" | "-") ~ unsignedReal <~ "[ij]".r ^^ {
        case re ~ "+" ~ im => (re, im)
        case re ~ "-" ~ im => (re, -im)
      }

  def units: Parser[String] =
      firstTerm ~ (divTerm | mulTerm).* ^^ {
        case first ~ rest => first + rest.mkString
      }

  def firstTerm =
    ( "1".? ~ divTerm ^^ { case one ~ term => one.getOrElse("") + term }
    | term
    )

  def mulTerm = "*" ~ term ^^ { case op ~ term => op + term }
  def divTerm = "/" ~ term ^^ { case op ~ term => op + term }

  def term =
      termName ~ exponent.? ^^ {
        case name ~ None      => name
        case name ~ Some(exp) => name + exp
      }

  def termName = """[A-Za-z'"][A-Za-z'"0-9]*""".r

  def exponent =
      "^" ~ "-".? ~ number ~ ("/" ~ number).? ^^ {
        case carat ~ None    ~ n ~ None            => carat     + n
        case carat ~ None    ~ n ~ Some(slash ~ d) => carat     + n + slash + d
        case carat ~ Some(m) ~ n ~ None            => carat + m + n
        case carat ~ Some(m) ~ n ~ Some(slash ~ d) => carat + m + n + slash + d
      }

  def number = """\d+""".r

  def array = arrND ^^ { case (elems, typ, shape) =>
    val data = TreeData(TArr(typ, shape.size))
    data.setArrayShape(shape: _*)
    for ((data, elem) <- data.flatIterator zip elems.iterator) {
      data.set(elem)
    }
    data
  }

  def arrND: Parser[(Array[Data], Type, List[Int])] =
    ( "[" ~> repsep(nonArrayData, ",") <~ "]" ^^ { elems =>
        val typ = if (elems.isEmpty) {
          TNone
        } else {
          val typ = elems(0).t
          require(elems forall (_.t == typ), s"all elements must be of type '$typ'")
          typ
        }
        (elems.toArray, typ, List(elems.size))
      }
    | "[" ~> repsep(arrND, ",") <~ "]" ^^ { subArrays =>
        // make sure all subarrays have the same shape and element type
        val (typ, shape) = (subArrays(0)._2, subArrays(0)._3)
        require(subArrays forall (_._2 == typ))
        require(subArrays forall (_._3 == shape))
        (subArrays.flatMap(_._1).toArray, typ, subArrays.size :: shape)
      }
    )

  def cluster = "(" ~> repsep(data, ",") <~ ")" ^^ { elems => Cluster(elems: _*) }
}

object Translate {

  def stringToBytes(s: String): Array[Byte] = {
    val ReStringElement = """'[^'\x00-\x1F\x7F-\xFF]*'|#[0-9]{1,3}""".r
    val matches = ReStringElement.findAllIn(s)
    var buf = Array.newBuilder[Byte]
    var lastWasString = false
    for (m <- matches) {
      if (m(0) == '\'') {
        if (lastWasString) {
          buf ++= m.dropRight(1).getBytes(UTF_8) // leave one quote in
        } else {
          buf ++= m.drop(1).dropRight(1).getBytes(UTF_8)
          lastWasString = true
        }
      } else {
        buf += m.drop(1).toInt.toByte
        lastWasString = false
      }
    }
    buf.result
  }
}


