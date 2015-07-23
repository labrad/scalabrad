package org.labrad.registry

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.labrad.data._
import org.labrad.types._
import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers


/**
 * Legacy file store for the registry format defined by the old delphi manager.
 * Uses % escapes for particular disallowed characters in key and file names
 * and a textual format for storing data.
 */
class DelphiFileStore(rootDir: File) extends FileStore(rootDir) {

  // special characters that we escape in key and directory filenames
  val decoded = """%/\:*?"<>|."""
  val encoded = """pfbcaqQlgPd"""

  val encodeMap = (decoded zip encoded).toMap
  val decodeMap = (encoded zip decoded).toMap

  /**
   * Encode arbitrary string in a format suitable for use as a filename.
   *
   * Use the encodeMap defined above to identify characters that need escaping.
   */
  override def encode(segment: String): String = {
    val b = new StringBuilder
    for (c <- segment) {
      encodeMap.get(c) match {
        case None => b += c
        case Some(c) => b ++= "%" + c
      }
    }
    b.toString
  }

  override def decode(segment: String): String = {
    val it = segment.iterator
    val b = new StringBuilder
    while (it.hasNext) {
      b += (it.next match {
        case '%' => decodeMap(it.next)
        case c => c
      })
    }
    b.toString
  }

  /**
   * Encode and decode data for storage in individual key files.
   */
  override def encodeData(data: Data): Array[Byte] = {
    DelphiFormat.dataToString(data).getBytes(UTF_8)
  }

  override def decodeData(bytes: Array[Byte]): Data = {
    DelphiFormat.stringToData(new String(bytes, UTF_8))
  }
}

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
    """('[^'\x00-\x1F\x7F-\xFF]*'|#[0-9]{1,3})+""".r ^^ { s => Bytes(DelphiFormat.stringToBytes(s)) }

  def time: Parser[Data] =
    """\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}(.\d{1,3})?""".r ~ """\d*(E-\d+)?""".r ^^ { case s ~ ignored => Time(DelphiFormat.DateFormat.parseDateTime(s).toDate) }

  def value: Parser[Data] =
      signedReal ~ (' ' ~> units).? ^^ { case num ~ unit => Value(num, unit.orElse(Some(""))) }

  def complex: Parser[Data] =
      complexNum ~ (' ' ~> units).? ^^ { case (re, im) ~ unit => Cplx(re, im, unit.orElse(Some(""))) }

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

  def array = arrND ^^ { case (elems, shape) =>
    val b = new DataBuilder
    b.array(shape: _*)
    for (elem <- elems) {
      b.add(elem)
    }
    b.result()
  }

  def arrND: Parser[(Array[Data], List[Int])] =
    ( "[" ~> repsep(nonArrayData, ",") <~ "]" ^^ { elems =>
        (elems.toArray, List(elems.size))
      }
    | "[" ~> repsep(arrND, ",") <~ "]" ^^ { subArrays =>
        // make sure all subarrays have the same shape and element type
        val shape = subArrays(0)._2
        require(subArrays forall (_._2 == shape))
        (subArrays.flatMap(_._1).toArray, subArrays.size :: shape)
      }
    )

  def cluster = "(" ~> repsep(data, ",") <~ ")" ^^ { elems => Cluster(elems: _*) }
}

object DelphiFormat {

  /**
   * Convert data from its delphi string format to labrad data.
   */
  def stringToData(s: String): Data = DelphiParsers.parseData(s)

  /**
   * Convert labrad data to its delphi-formatted string represenataion.
   */
  def dataToString(data: Data): String = {
    data.t match {
      case TNone => "_"

      case TBool => data.getBool.toString

      case TInt =>
        data.getInt match {
          case i if i >= 0 => "+" + i
          case i => i.toString
        }

      case TUInt => data.getUInt.toString

      case TValue(u) =>
        doubleToString(data.getValue) + (u match {
          case Some("") | None => ""
          case Some(unit) => " " + unit
        })

      case TComplex(u) =>
        val (re, im) = (data.getReal, data.getImag)
        doubleToString(re) + (if (im >= 0) "+" else "") +
        doubleToString(im) + "i" + (u match {
          case Some("") | None => ""
          case Some(unit) => " " + unit
        })

      case TTime =>
        DateFormat.print(data.getTime.toDateTime)

      case TStr => bytesToString(data.getBytes)

      case TArr(elem, depth) =>
        val shape = data.arrayShape
        val idx = Array.ofDim[Int](depth)
        val sb = new StringBuilder
        val it = data.flatIterator
        def buildString(k: Int) {
          sb += '['
          for (i <- 0 until shape(k)) {
            idx(k) = i
            if (i > 0) sb += ','
            if (k == shape.length - 1)
              sb ++= dataToString(it.next)
            else
              buildString(k + 1)
          }
          sb += ']'
        }
        buildString(0)
        sb.toString

      case TCluster(_*) =>
        "(" + data.clusterIterator.map(dataToString).mkString(",") + ")"

      case TError(_) =>
        sys.error("cannot to encode error in delphi text format")
    }
  }

  /**
   * Convert a double to a delphi-formatted string.
   */
  def doubleToString(x: Double): String = {
    x match {
      case Double.NaN => "NAN.0"
      case Double.PositiveInfinity => "INF.0"
      case Double.NegativeInfinity => "-INF.0"
      case x =>
        val s = x.toString
        if (s.contains("e") || s.contains("E") || s.contains(".")) s else s + ".0"
    }
  }

  val DateFormat = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss.SSS")

  /**
   * Convert a delphi-formatted string into an array of bytes.
   */
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

  /**
   * Convert an array of bytes into a delphi-formatted string.
   */
  def bytesToString(bytes: Array[Byte]): String = {
    if (bytes.isEmpty) return "''"
    val b = new StringBuilder
    var quoted = false
    for (x <- bytes.iterator.map(_ & 0xff)) {
      if (x <= 0x1f || x >= 0x7f) {
        if (quoted) {
          b ++= "'"
          quoted = false
        }
        b ++= s"#$x"
      } else {
        if (!quoted) {
          b ++= "'"
          quoted = true
        }
        x match {
          case QUOTE => b ++= "''"
          case x => b += x.toChar
        }
      }
    }
    if (quoted) b ++= "'"
    b.toString
  }

  val QUOTE = '\''.toInt
}


