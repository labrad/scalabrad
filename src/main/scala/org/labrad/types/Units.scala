package org.labrad.types

import scala.collection.mutable
import scala.math

trait RatioIsFractional extends Fractional[Ratio] {
  def plus(x: Ratio, y: Ratio) = Ratio(x.num * y.denom + y.num * x.denom, x.denom * y.denom)
  def minus(x: Ratio, y: Ratio) = Ratio(x.num * y.denom - y.num * x.denom, x.denom * y.denom)
  def times(x: Ratio, y: Ratio) = Ratio(x.num * y.num, x.denom * y.denom)
  def div(x: Ratio, y: Ratio) = Ratio(x.num * y.denom, x.denom * y.num)
  def negate(x: Ratio) = Ratio(-x.num, x.denom)
  def fromInt(x: Int) = Ratio(x)
  def toInt(x: Ratio): Int = (x.num / x.denom).toInt
  def toLong(x: Ratio): Long = x.num.toLong / x.denom.toLong
  def toFloat(x: Ratio) = x.num.toFloat / x.denom.toFloat
  def toDouble(x: Ratio) = x.num.toDouble / x.denom.toDouble
}

trait RatioIsOrdered extends Ordering[Ratio] {
  def compare(a: Ratio, b: Ratio): Int = (a.num.toLong * b.denom) compare (b.num.toLong * a.denom)
}

class Ratio protected(val num: Long, val denom: Long = 1) extends Ordered[Ratio] {

  def compare(that: Ratio): Int = (BigInt(num) * that.denom) compare (BigInt(that.num) * denom)

  def *(other: Ratio) = Ratio(num * other.num, denom * other.denom)
  def *(other: Long) = Ratio(num * other, denom)

  def /(other: Ratio) = Ratio(num * other.denom, denom * other.num)
  def /(other: Long) = Ratio(num, denom * other)

  def +(other: Ratio) = Ratio(num * other.denom + other.num * denom, denom * other.denom)
  def +(other: Long) = Ratio(num + other * denom, denom)

  def -(other: Ratio) = Ratio(num * other.denom - other.num * denom, denom * other.denom)
  def -(other: Long) = Ratio(num - other * denom, denom)

  def unary_- = Ratio(-num, denom)

  def toDouble = num.toDouble / denom.toDouble

  override def toString = if (denom == 1) s"$num" else s"$num/$denom"

  override def equals(other: Any) = other match {
    case Ratio(n, d) => n == num && d == denom
    case n: Long => n == num && 1 == denom
    case n: Int => n == num && 1 == denom
    case n: Short => n == num && 1 == denom
    case _ => false
  }

  override def hashCode() = (41 * (41 + num) + denom).toInt
}

object Ratio {
  def apply(num: Long, denom: Long = 1): Ratio = {
    require(denom != 0)
    val s = math.signum(denom)
    val f = gcd(num, denom)
    new Ratio(s*num/f, s*denom/f)
  }

  def unapply(r: Ratio): Option[(Long, Long)] = Some((r.num, r.denom))

  def gcd(a: Long, b: Long): Long = {
    if (a < 0) gcd(-a, b)
    else if (b < 0) gcd(a, -b)
    else if (b > a) gcd(b, a)
    else if (b == 0) a
    else gcd(b, a % b)
  }

  implicit object RatioIsFractional extends RatioIsFractional with RatioIsOrdered
}


object Units {
  /** create an object to convert number between the given units */
  def optConvert(fromUnit: String, toUnit: String): Option[Double => Double] = {
    None

    var from = parse(fromUnit)
    var to = parse(toUnit)

    if (from == to) return None

    val toSI = from match {
      case Seq((name, Ratio(1, 1))) =>
        nonlinearUnits.get(name) map { u => from = u.SI; u.toSI }
      case _ => None
    }

    val fromSI = to match {
      case Seq((name, Ratio(1, 1))) =>
        nonlinearUnits.get(name) map { u => to = u.SI; u.fromSI }
      case _ => None
    }

    val terms = combineTerms(from, to)
    val factor = getConversionFactor(terms, fromUnit, toUnit)

    (toSI, fromSI) match {
      case (Some(toSI), Some(fromSI)) => Some(x => fromSI(factor * toSI(x)))
      case (Some(toSI), None        ) => Some(x =>       (factor * toSI(x)))
      case (None,       Some(fromSI)) => Some(x => fromSI(factor *     (x)))
      case (None,       None        ) => Some(x =>       (factor *     (x)))
    }
  }

  /** create an object to convert number between the given units */
  def convert(from: String, to: String): Double => Double = optConvert(from, to).getOrElse((x) => x)

  /** Combine two lists of terms, adding common exponents and removing terms with exponent 0 */
  def combineTerms(from: Seq[Term], to: Seq[Term]): Seq[Term] =
    (from ++ (to map { case (name, exp) => (name, -exp) }))
      .groupBy(_._1) // by name
      .map { case (name, terms) => name -> terms.map(_._2) } // convert term list to exponent list
      .map { case (name, exps) => name -> exps.foldLeft(Ratio(0))(_ + _) } // sum exponents
      .filter { case (name, exp) => exp != 0 } // drop terms with exponent zero
      .toSeq
      .sorted

  /** Get conversion factor based on the given term list */
  def getConversionFactor(terms: Seq[Term], from: String, to: String): Double = {
    var factor = 1.0
    val sums = Array.fill(9)(Ratio(0))
    for ((name, exp) <- terms) {
      val (fact, dims) = linearUnits(name)
      factor *= math.pow(fact, exp.toDouble)
      for (i <- 0 until 9)
        sums(i) += exp * dims(i)
    }
    require(sums.forall(_ == 0), s"cannot convert units '$from' to '$to'")
    factor
  }

  type Term = (String, Ratio)

  /** Parse a unit string into a list of tokens with rational exponents */
  def parse(units: String): Seq[Term] = Parsers.parseUnit(units)


  // name    prefixable     factor   m  kg   s   A   K mol  cd rad  sr
  val baseUnits = Seq(
    ("m",       true,          1.0,  1,  0,  0,  0,  0,  0,  0,  0,  0),
    ("g",       true,       1.0e-3,  0,  1,  0,  0,  0,  0,  0,  0,  0),
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
    ("erg",     true,       1.0e-7,  2,  1, -2,  0,  0,  0,  0,  0,  0),
    ("J",       true,          1.0,  2,  1, -2,  0,  0,  0,  0,  0,  0),
    ("dyn",     true,       1.0e-5,  1,  1, -2,  0,  0,  0,  0,  0,  0),
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
    ("bar",     true,        1.0e5, -1,  1, -2,  0,  0,  0,  0,  0,  0),
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


  case class NLUnit(SI: Seq[Term], toSI: Double => Double, fromSI: Double => Double)

  def nonlinearUnits = Map(
     "dBW" -> NLUnit(SI = parse("W"), toSI =  dBW => math.pow(10, dBW/10),        fromSI = W => 10*math.log10(W)),
     "dBm" -> NLUnit(SI = parse("W"), toSI =  dBm => math.pow(10, dBm/10) / 1000, fromSI = W => 10*math.log10(W*1000)),
    "degF" -> NLUnit(SI = parse("K"), toSI = degF => (degF + 459.67) / 1.8,       fromSI = K => K * 1.8 - 459.67),
    "degC" -> NLUnit(SI = parse("K"), toSI = degC => degC + 273.15,               fromSI = K => K - 273.15)
  )
}
