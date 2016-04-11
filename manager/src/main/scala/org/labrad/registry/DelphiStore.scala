package org.labrad.registry

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.labrad.data._
import org.labrad.types._
import scala.util.matching.Regex


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
  override def encodeData(data: Data, textOpt: Option[String]): Array[Byte] = {
    DelphiFormat.dataToString(data).getBytes(UTF_8)
  }

  override def decodeData(bytes: Array[Byte]): (Data, Option[String]) = {
    (DelphiFormat.stringToData(new String(bytes, UTF_8)), None)
  }
}

/**
 * Parsers for the string representation used by the legacy delphi code.
 */
object DelphiParsers {

  import fastparse.noApi._
  import org.labrad.util.Parsing._
  import org.labrad.util.Parsing.AllowWhitespace._

  def parseData(s: String): Data = parseOrThrow(dataAll, s)

  val dataAll: Parser[Data] =
    P( Whitespace ~ data ~ Whitespace ~ End )

  val data: Parser[Data] =
    P( nonArrayData | array )

  val nonArrayData: Parser[Data] =
    P( none | bool | complex | value | time | int | uint | bytes | string | cluster )

  val none: Parser[Data] =
    P( "_" ).map { _ => Data.NONE }

  val bool: Parser[Data] =
    P( Re("[Tt]rue").map { _ => Bool(true) }
     | Re("[Ff]alse").map { _ => Bool(false) }
     )

  val int: Parser[Data] =
    P( "+" ~ unsignedInt.map { x => Integer(x.toInt) }
     | "-" ~ unsignedInt.map { x => Integer(-x.toInt) }
     )

  val uint: Parser[Data] =
    P( unsignedInt.map { num => UInt(num) } )

  val unsignedInt: Parser[Long] =
    P( number.!.map(_.toLong) )

  val bytes: Parser[Data] =
    P( "b" ~ bytesLiteral ).map { bytes => Bytes(bytes) }

  val string: Parser[Data] =
    P( bytesLiteral ).map { bytes => Str(new String(bytes, UTF_8)) }

  val bytesLiteral: Parser[Array[Byte]] =
    P( Re("""('[^'\x00-\x1F\x7F-\xFF]*'|#[0-9]{1,3})+""").! ).map { s => DelphiFormat.stringToBytes(s) }

  val time: Parser[Data] =
    P( Re("""\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}(.\d{1,3})?""").! ~ Re("""\d*(E-\d+)?""") ).map { s => Time(DelphiFormat.DateFormat.parseDateTime(s).toDate) }

  val value: Parser[Data] =
    P( signedReal ~ units.!.? ).map { case (num, unit) => Value(num, unit.orElse(Some(""))) }

  val complex: Parser[Data] =
    P( complexNum ~ units.!.? ).map { case (re, im, unit) => Cplx(re, im, unit.orElse(Some(""))) }

  val signedReal: Parser[Double] =
    P( "+" ~ unsignedReal.map { x => x }
     | "-" ~ unsignedReal.map { x => -x }
     | unsignedReal
     )

  val unsignedReal: Parser[Double] =
    P( Token("NAN.0").map { _ => Double.NaN }
     | Token("INF.0").map { _ => Double.PositiveInfinity }
     | Re("""(\d*\.\d+|\d+(\.\d*)?)[eE][+-]?\d+""").!.map { _.toDouble }
     | Re("""\d*\.\d+""").!.map { _.toDouble }
     )

  val complexNum: Parser[(Double, Double)] =
    P( (signedReal ~ ("+" | "-").! ~ unsignedReal ~ CharIn("ij")).map {
        case (re, "+", im) => (re, im)
        case (re, "-", im) => (re, -im)
       }
     )

  val units = P( firstTerm ~ (divTerm | mulTerm).rep )
  val firstTerm = P( "1".? ~ divTerm | term )
  val mulTerm = P( "*" ~ term )
  val divTerm = P( "/" ~ term )
  val term = P( termName ~ exponent.? )
  val termName = P( Re("""[A-Za-z'"][A-Za-z'"0-9]*""") )
  val exponent = P( "^" ~ "-".? ~ number ~ ("/" ~ number).? )
  val number = P( Re("""\d+""") )

  val array: Parser[Data] = P( arrND ).map { case (elems, shape) =>
    val b = new DataBuilder
    b.array(shape: _*)
    for (elem <- elems) {
      b.add(elem)
    }
    b.result()
  }

  val arrND: Parser[(Array[Data], List[Int])] =
    P( ("[" ~ nonArrayData.rep(sep = ",") ~ "]").map { elems =>
         (elems.toArray, List(elems.size))
       }
     | ("[" ~ arrND.rep(sep = ",") ~ "]").map { subArrays =>
         // make sure all subarrays have the same shape and element type
         val shape = subArrays(0)._2
         require(subArrays forall (_._2 == shape))
         (subArrays.flatMap(_._1).toArray, subArrays.size :: shape)
       }
     )

  val cluster =
    P( "(" ~/ data.rep(sep = ",").map(xs => Cluster(xs: _*)) ~ ")" )
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

      case TStr => bytesToString(data.getString.getBytes(UTF_8))
      case TBytes => "b" + bytesToString(data.getBytes)

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


