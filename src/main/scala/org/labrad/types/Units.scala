package org.labrad
package types

import scala.collection.mutable
import scala.math


class Ratio protected(val num: Int, val denom: Int = 1) {
  def *(other: Ratio) = Ratio(num * other.num, denom * other.denom)
  def *(other: Int) = Ratio(num * other, denom)
  
  def /(other: Ratio) = Ratio(num * other.denom, denom * other.num)
  def /(other: Int) = Ratio(num, denom * other)
  
  def +(other: Ratio) = Ratio(num * other.denom + other.num * denom, denom * other.denom)
  def +(other: Int) = Ratio(num + other * denom, denom)
  
  def -(other: Ratio) = Ratio(num * other.denom - other.num * denom, denom * other.denom)
  def -(other: Int) = Ratio(num - other * denom, denom)
  
  def toDouble = num.toDouble / denom.toDouble
  
  override def toString = denom match {
    case 1 => num.toString
    case _ => num + "/" + denom
  }
  
  override def equals(other: Any) = other match {
    case Ratio(n, d) => n == num && d == denom
    case n: Int => n == num && 1 == denom
    case _ => false
  }
  
  override def hashCode() = (41 * (41 + num) + denom)
}

object Ratio {
  def apply(num: Int, denom: Int = 1): Ratio = {
    require(denom != 0)
    val s = math.signum(denom)
    val f = gcd(num, denom)
    new Ratio(s*num/f, s*denom/f)
  }
  
  def unapply(r: Ratio): Option[(Int, Int)] = Some((r.num, r.denom))
  
  def gcd(a: Int, b: Int): Int = {
    if (a < 0) gcd(-a, b)
    else if (b < 0) gcd(a, -b)
    else if (b > a) gcd(b, a)
    else if (b == 0) a
    else gcd(b, a % b)
  }
}


object Units {
    
  def convert(fromUnit: String, toUnit: String): Double => Double = {
    var from = parse(fromUnit)
    var to = parse(toUnit)
    
    if (from == to) return (x: Double) => x

    val toSI = from match {
      case Seq((name, Ratio(1, 1))) =>
        nonlinearUnits.get(name) match {
          case Some(unit) =>
            from = unit.SI
            Some(unit.toSI)
          case None => None
        }
      case _ => None
    }
    
    val fromSI = to match {
      case Seq((name, Ratio(1, 1))) =>
        nonlinearUnits.get(name) match {
          case Some(unit) =>
            to = unit.SI
            Some(unit.fromSI)
          case None => None
        }
      case _ => None
    }
    
    val tokens = combineTokens(from, to)
    val factor = getConversionFactor(tokens, fromUnit, toUnit)
    
    (toSI, fromSI) match {
      case (Some(toSI), Some(fromSI)) => (x: Double) => fromSI(factor * toSI(x))
      case (Some(toSI), None        ) => (x: Double) =>       (factor * toSI(x))
      case (None,       Some(fromSI)) => (x: Double) => fromSI(factor *     (x))
      case (None,       None        ) => (x: Double) =>       (factor *     (x))
    }
  }
  
  /** Combine two lists of tokens, adding exponents for common tokens and removing any with exponent == 0 */
  def combineTokens(from: Seq[Token], to: Seq[Token]): Seq[Token] = {
    val exps = mutable.Map.empty[String, Ratio]
    for ((name, exp) <- from) exps(name) = exps.get(name).getOrElse(Ratio(0)) + exp
    for ((name, exp) <-   to) exps(name) = exps.get(name).getOrElse(Ratio(0)) - exp
    for {
      name <- exps.keys.toSeq.sorted
      exp = exps(name) if exp.num != 0
    } yield (name, exp)
  }
  
  /** Get conversion factor based on the given token list */
  def getConversionFactor(tokens: Seq[Token], from: String, to: String): Double = {
    var factor = 1.0
    val sums = Array.fill(9)(Ratio(0))
    for ((name, exp) <- tokens) {
      val (fact, dims) = linearUnits(name)
      factor *= math.pow(fact, exp.toDouble)
      for (i <- 0 until 9)
        sums(i) += exp * dims(i)
    }
    require(sums forall (_ == 0), "cannot convert %s to %s".format(from, to))
    factor
  }
  
  
  type Token = (String, Ratio)
  
  /** Parse a unit string into a list of tokens with rational exponents */
  def parse(units: String): Seq[Token] = {
    sealed trait ParseState
    case object Start extends ParseState
    case object NeedSplit extends ParseState
    case object NeedUnit extends ParseState
    case object InUnit extends ParseState
    case object NeedSplitOrExp extends ParseState
    case object NeedExpNumOrSign extends ParseState
    case object NeedExpNum extends ParseState
    case object InExpNum extends ParseState
    case object InRealExp extends ParseState
    case object NeedUnitOrExpDenom extends ParseState
    case object NeedSplitOrExpDenom extends ParseState
    case object InExpDenom extends ParseState
    
    object DIGIT {
      def unapply(c: Char): Option[Int] = c match {
        case '0'|'1'|'2'|'3'|'4'|'5'|'6'|'7'|'8'|'9' => Some(c.toString.toInt)
        case _ => None
      }
    }
    
    object CHAR {
      def unapply(c: Char): Boolean = c match {
        case 'A'|'B'|'C'|'D'|'E'|'F'|'G'|'H'|'I'|'J'|'K'|'L'|'M'|'N'|'O'|'P'|'Q'|'R'|'S'|'T'|'U'|'V'|'W'|'X'|'Y'|'Z'|
             'a'|'b'|'c'|'d'|'e'|'f'|'g'|'h'|'i'|'j'|'k'|'l'|'m'|'n'|'o'|'p'|'q'|'r'|'s'|'t'|'u'|'v'|'w'|'x'|'y'|'z'|
             '\''|'"' => true
        case _ => false
      }
    }
    
    object SPACE {
      def unapply(c: Char): Boolean = c match {
        case ' '|'\t' => true
        case _ => false
      }
    }
    
    def die = error("unit parsing error: " + units)
    
    var state: ParseState = Start
    
    val tokens = Seq.newBuilder[Token]
    var curTok = ""
    var curNum = 1
    var curDenom = 1
    var negExp = false
    
    for (c <- units) {
      var addTok = false
      state match {
        case Start =>
          c match {
            case CHAR() =>
              curTok = c.toString
              curNum = 1
              curDenom = 1
              state = InUnit
            case '1' =>
              state = NeedSplit
            case '*'|'/' =>
              negExp = c == '/'
              state = NeedUnit
            case SPACE() =>
            case _ => die
          }
  
        case NeedSplit =>
          c match {
            case '*'|'/' =>
              if (!curTok.isEmpty) {
                if (negExp) curNum *= -1
                addTok = true
              }
              negExp = c == '/'
              state = NeedUnit
            case SPACE() =>
            case _ => die
          }
  
        case NeedUnit =>
          c match {
            case CHAR() =>
              curTok = c.toString
              curNum = 1
              curDenom = 1
              state = InUnit
            case SPACE() =>
            case _ => die
          }
  
        case InUnit =>
          c match {
            case CHAR() =>
              curTok += c
            case SPACE() =>
              state = NeedSplitOrExp
            case '*'|'/' =>
              if (negExp) curNum *= -1
              addTok = true
              negExp = c == '/'
              state = NeedUnit
            case '^' =>
              state = NeedExpNumOrSign
            case _ => die
          }
  
        case NeedSplitOrExp =>
          c match {
            case '*'|'/' =>
              if (!curTok.isEmpty) {
                if (negExp) curNum *= -1
                addTok = true
              }
              negExp = c == '/'
              state = NeedUnit
            case '^' =>
              state = NeedExpNumOrSign
            case SPACE() =>
            case _ => die
          }
  
        case NeedExpNumOrSign =>
          c match {
            case '-' =>
              negExp = !negExp
              state = NeedExpNum
            case DIGIT(d) =>
              curNum = d
              state = InExpNum
            case SPACE() =>
            case _ => die
          }
  
        case NeedExpNum =>
          c match {
            case DIGIT(d) =>
              curNum = d
              state = InExpNum
            case SPACE() =>
            case _ => die
          }
  
        case InExpNum =>
          c match {
            case DIGIT(d) =>
              curNum = curNum * 10 + d
            case '.' =>
              state = InRealExp
            case '/' =>
              if (negExp) curNum *= -1
              state = NeedUnitOrExpDenom
            case '*' =>
              if (negExp) curNum *= -1
              negExp = false
              addTok = true
              state = NeedUnit
            case SPACE() =>
              if (negExp) curNum *= -1
              negExp = false
              state = NeedSplitOrExpDenom
            case _ => die
          }
  
        case InRealExp =>
          c match {
            case DIGIT(d) =>
              curNum = curNum * 10 + d
              curDenom = curDenom * 10
            case SPACE() =>
              state = NeedSplit
            case '*'|'/' =>
              if (negExp) curNum *= -1
              addTok = true;
              negExp = c == '/'
              state = NeedUnit
            case _ => die
          }
  
        case NeedSplitOrExpDenom =>
          c match {
            case '*' =>
              addTok = true
              negExp = false
              state = NeedUnit
            case '/' =>
              state = NeedUnitOrExpDenom
            case SPACE() =>
            case _ => die
          }
  
        case NeedUnitOrExpDenom =>
          c match {
            case CHAR() =>
              tokens += ((curTok, Ratio(curNum, curDenom)))
              curTok = c.toString
              curNum = 1
              curDenom = 1
              negExp = true
              state = InUnit
            case DIGIT(d) =>
              curDenom = d
              negExp = false
              state = InExpDenom
            case ' '|'\t' =>
            case _ => die
          }
  
        case InExpDenom =>
          c match {
            case DIGIT(d) =>
              curDenom = curDenom * 10 + d
            case '*'|'/' =>
              addTok = true
              negExp = c == '/'
              state = NeedUnit
            case SPACE() =>
              state = NeedSplit
            case _ => die
          }
      }
      if (addTok) tokens += ((curTok, Ratio(curNum, curDenom)))
    }
    state match {
      case NeedUnit | NeedUnitOrExpDenom | NeedExpNum => error("invalid unit string '" + units + "'")
      case _ =>
    }
    if (!curTok.isEmpty) {
      if (negExp) curNum *= -1;
      tokens += ((curTok, Ratio(curNum, curDenom)))
    }
    tokens.result
  }
  

  // name    prefixable     factor   m  kg   s   A   K mol  cd rad  sr
  val baseUnits = Seq(
    ("m",       true,          1.0,  1,  0,  0,  0,  0,  0,  0,  0,  0),
    ("g",       true,        0.001,  0,  1,  0,  0,  0,  0,  0,  0,  0),
    ("s",       true,          1.0,  0,  0,  1,  0,  0,  0,  0,  0,  0),
    ("A",       true,          1.0,  0,  0,  0,  1,  0,  0,  0,  0,  0),
    ("K",       true,          1.0,  0,  0,  0,  0,  1,  0,  0,  0,  0),
    ("mol",     true,          1.0,  0,  0,  0,  0,  0,  1,  0,  0,  0),
    ("cd",      true,          1.0,  0,  0,  0,  0,  0,  0,  1,  0,  0),
    ("rad",     true,          1.0,  0,  0,  0,  0,  0,  0,  0,  1,  0),
    ("sr",      true,          1.0,  0,  0,  0,  0,  0,  0,  0,  0,  1),
    ("Bq",      true,          1.0,  0,  0, -1,  0,  0,  0,  0,  0,  0),
    ("Ci",      true,       3.7e10,  0,  0, -1,  0,  0,  0,  0,  0,  0),
    ("acre",    false,      4046.9,  2,  0,  0,  0,  0,  0,  0,  0,  0),
    ("a",       true,        100.0,  2,  0,  0,  0,  0,  0,  0,  0,  0),
    ("F",       true,          1.0, -2, -1,  4,  2,  0,  0,  0,  0,  0),
    ("C",       true,          1.0,  0,  0,  1,  1,  0,  0,  0,  0,  0),
    ("S",       true,          1.0, -2, -1,  3,  2,  0,  0,  0,  0,  0),
    ("V",       true,          1.0,  2,  1, -3, -1,  0,  0,  0,  0,  0),
    ("Ohm",     true,          1.0,  2,  1, -3, -2,  0,  0,  0,  0,  0),
    ("Btu",     false,      1055.1,  2,  1, -2,  0,  0,  0,  0,  0,  0),
    ("cal",     true,       4.1868,  2,  1, -2,  0,  0,  0,  0,  0,  0),
    ("eV",      true,   1.6022e-19,  2,  1, -2,  0,  0,  0,  0,  0,  0),
    ("erg",     true,         1e-7,  2,  1, -2,  0,  0,  0,  0,  0,  0),
    ("J",       true,          1.0,  2,  1, -2,  0,  0,  0,  0,  0,  0),
    ("dyn",     true,      0.00001,  1,  1, -2,  0,  0,  0,  0,  0,  0),
    ("N",       true,          1.0,  1,  1, -2,  0,  0,  0,  0,  0,  0),
    ("ozf",     false,     0.27801,  1,  1, -2,  0,  0,  0,  0,  0,  0),
    ("lbf",     false,      4.4482,  1,  1, -2,  0,  0,  0,  0,  0,  0),
    ("Hz",      true,          1.0,  0,  0, -1,  0,  0,  0,  0,  0,  0),
    ("ft",      false,      0.3048,  1,  0,  0,  0,  0,  0,  0,  0,  0),
    ("in",      false,      0.0254,  1,  0,  0,  0,  0,  0,  0,  0,  0),
    ("mi",      false,      1609.3,  1,  0,  0,  0,  0,  0,  0,  0,  0),
    ("nit",     true,          1.0, -2,  0,  0,  0,  0,  0,  1,  0,  0),
    ("nits",    true,          1.0, -2,  0,  0,  0,  0,  0,  1,  0,  0),
    ("sb",      true,      10000.0, -2,  0,  0,  0,  0,  0,  1,  0,  0),
    ("fc",      false,      10.764, -2,  0,  0,  0,  0,  0,  1,  0,  1),
    ("lx",      true,          1.0, -2,  0,  0,  0,  0,  0,  1,  0,  1),
    ("phot",    true,      10000.0, -2,  0,  0,  0,  0,  0,  1,  0,  1),
    ("lm",      true,          1.0,  0,  0,  0,  0,  0,  0,  1,  0,  1),
    ("Mx",      true,         1e-8,  2,  1, -2, -1,  0,  0,  0,  0,  0),
    ("Wb",      true,          1.0,  2,  1, -2, -1,  0,  0,  0,  0,  0),
    ("G",       true,       0.0001,  0,  1, -2, -1,  0,  0,  0,  0,  0),
    ("T",       true,          1.0,  0,  1, -2, -1,  0,  0,  0,  0,  0),
    ("H",       true,          1.0,  2,  1, -2, -2,  0,  0,  0,  0,  0),
    ("u",       true,   1.6605e-27,  0,  1,  0,  0,  0,  0,  0,  0,  0),
    ("lb",      false,     0.45359,  0,  1,  0,  0,  0,  0,  0,  0,  0),
    ("slug",    false,      14.594,  0,  1,  0,  0,  0,  0,  0,  0,  0),
    ("deg",     false,    0.017453,  0,  0,  0,  0,  0,  0,  0,  1,  0),
    ("'",       false,  0.00029089,  0,  0,  0,  0,  0,  0,  0,  1,  0),
    ("\"",      false,   4.8481e-6,  0,  0,  0,  0,  0,  0,  0,  1,  0),
    ("hp",      false,       745.7,  2,  1, -3,  0,  0,  0,  0,  0,  0),
    ("W",       true,          1.0,  2,  1, -3,  0,  0,  0,  0,  0,  0),
    ("atm",     false,    1.0133e5, -1,  1, -2,  0,  0,  0,  0,  0,  0),
    ("bar",     true,          1e5, -1,  1, -2,  0,  0,  0,  0,  0,  0),
    ("Pa",      true,          1.0, -1,  1, -2,  0,  0,  0,  0,  0,  0),
    ("torr",    true,       133.32, -1,  1, -2,  0,  0,  0,  0,  0,  0),
    ("mmHg",    false,      133.32, -1,  1, -2,  0,  0,  0,  0,  0,  0),
    ("degC",    false,         1.0,  0,  0,  0,  0,  1,  0,  0,  0,  0),
    ("degF",    false,     5.0/9.0,  0,  0,  0,  0,  1,  0,  0,  0,  0),
    ("d",       false,     86400.0,  0,  0,  1,  0,  0,  0,  0,  0,  0),
    ("h",       false,      3600.0,  0,  0,  1,  0,  0,  0,  0,  0,  0),
    ("min",     false,        60.0,  0,  0,  1,  0,  0,  0,  0,  0,  0),
    ("y",       true,     3.1557e7,  0,  0,  1,  0,  0,  0,  0,  0,  0),
    ("gal",     false,   0.0037854,  3,  0,  0,  0,  0,  0,  0,  0,  0),
    ("l",       true,        0.001,  3,  0,  0,  0,  0,  0,  0,  0,  0),
    ("pint",    false,  0.00047318,  3,  0,  0,  0,  0,  0,  0,  0,  0),
    ("qt",      false,  0.00094635,  3,  0,  0,  0,  0,  0,  0,  0,  0)
  )

  // pre  factor
  val prefixes = Seq(
    ("Y",   1e24),
    ("Z",   1e21),
    ("E",   1e18),
    ("P",   1e15),
    ("T",   1e12),
    ("G",    1e9),
    ("M",    1e6),
    ("k",    1e3),
    ("h",    1e2),
    ("da",   1e1),
    ("d",   1e-1),
    ("c",   1e-2),
    ("m",   1e-3),
    ("u",   1e-6),
    ("n",   1e-9),
    ("p",  1e-12),
    ("f",  1e-15),
    ("a",  1e-18),
    ("z",  1e-21),
    ("y",  1e-24)
  )

  val linearUnits: Map[String, (Double, Seq[Ratio])] = {
    val builder = Map.newBuilder[String, (Double, Seq[Ratio])]
    for ((name, prefixed, factor, m, kg, s, amps, kelvin, mol, cd, rad, sr) <- baseUnits) {
      val dims = Seq(m, kg, s, amps, kelvin, mol, cd, rad, sr) map (Ratio(_))
      builder += name -> (factor, dims)
      if (prefixed)
        for ((prefix, pre) <- prefixes)
          builder += (prefix + name) -> (pre * factor, dims)
    }
    builder.result
  }
  

  case class NLUnit(SI: Seq[Token], toSI: Double => Double, fromSI: Double => Double)

  def nonlinearUnits = Map(
    "dBW" -> NLUnit(SI = parse("W"), toSI = convertdBWtoW, fromSI = convertWtodBW),
    "dBm" -> NLUnit(SI = parse("W"), toSI = convertdBmtoW, fromSI = convertWtodBm),
    "degF" -> NLUnit(SI = parse("K"), toSI = convertdegFtoK, fromSI = convertKtodegF),
    "degC" -> NLUnit(SI = parse("K"), toSI = convertdegCtoK, fromSI = convertKtodegC)
  )
  
  def convertdBWtoW(dBW: Double): Double = dBW match {
    case Double.PositiveInfinity => Double.PositiveInfinity
    case Double.NegativeInfinity => 0
    case _ => math.pow(10, dBW/10)
  }
  
  def convertWtodBW(W: Double): Double =
    if (W <= 0) Double.NaN else 10*math.log10(W)
  
  def convertdBmtoW(dBm: Double): Double = dBm match {
    case Double.PositiveInfinity => Double.PositiveInfinity
    case Double.NegativeInfinity => 0
    case _ => math.pow(10, dBm/10) / 1000
  }
  
  def convertWtodBm(W: Double): Double =
    if (W <= 0) Double.NaN else 10*math.log10(W*1000)
  
  def convertdegFtoK(degF: Double) = (degF + 459.67) / 1.8
  def convertKtodegF(K: Double) = K * 1.8 - 459.67
  
  def convertdegCtoK(degC: Double) = degC + 273.15
  def convertKtodegC(K: Double) = K - 273.15
}