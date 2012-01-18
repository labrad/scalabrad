/*
 * Copyright 2008 Matthew Neeley
 * 
 * This file is part of JLabrad.
 *
 * JLabrad is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 * 
 * JLabrad is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with JLabrad.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.labrad
package types


// TODO: separate type matchers (abstract) from types (concrete)


// A type descriptor consists of a tag and a type parsed from that tag
// this is because parsing can remove some information, eg comments and whitespace
class TypeDescriptor(val tag: String) {
  val typ = Type(tag)
  
  def accepts(other: String) = typ accepts other
  def accepts(other: Type) = typ accepts other
  def accepts(other: TypeDescriptor) = typ accepts other.typ
  
  override def toString = tag
}

object TypeDescriptor {
  def apply(tag: String) = new TypeDescriptor(tag)
  def apply(typ: Type) = new TypeDescriptor(typ.toString)
}


sealed trait Type {
  def accepts(tag: String): Boolean = accepts(Type(tag))
  def accepts(typ: Type): Boolean

  def expanded: List[Type] = List(this) // TODO: get rid of type expansion once APIs support TChoice
  
  /**
   * Returns a compact string describing this type object.
   * This string representation is used in flattening and is also
   * suitable for passing to the Data constructor to create a new
   * LabRAD data object.
   */
  def toString: String

  /**
   * Indicates whether this is an abstract type, that is,
   * one that can not actually be instantiated as a data object.
   * Abstract types are still useful for pattern-matching accepted types.
   */
  def isAbstract: Boolean
  
  /**
   * Indicates whether this type object represents data whose byte length is fixed.
   * @return
   */
  def fixedWidth: Boolean

  /**
   * Gives the width of the byte string for data of this type.  Note that if this is
   * not a fixed-width type, then some of the bytes here are pointers to variable-length
   * sections.
   * @return
   */
  def dataWidth: Int
}

object Type {
  // types used in parsing and unparsing packets
  val HEADER = Type("(ww)iww")
  val PACKET = Type("(ww)iws")
  val RECORD = Type("wss")

  private class Buffer(var s: String) {
    def get(i: Int = 1): String = {
      val temp = s.substring(0, i)
      s = s.substring(i)
      temp
    }
    def getChar = get().charAt(0)

    def peek(i: Int = 1) = s.substring(0, i)
    def peekChar = s.charAt(0)

    def skip(i: Int = 1) { s = s.substring(i) }
    def skipWhitespace { s = s.replaceFirst("[,\\s]*", "") }

    def isEmpty = length == 0
    def length = s.length
    override def toString = s
  }

  def apply(tag: String): Type = {
    val tb = new Buffer(stripComments(tag))
    val buf = Seq.newBuilder[Type]
    while (!tb.isEmpty) buf += parseSingleType(tb)
    buf.result match {
      case Seq() => TEmpty
      case Seq(t) => t
      case subTypes => TCluster(subTypes: _*)
    }
  }

  private def stripComments(tag: String): String =
    tag.split(":")(0) // strip off any trailing comments
       .replaceAll("\\{[^\\{\\}]*\\}", "") // remove anything in brackets

  private def parseSingleType(tb: Buffer): Type = {
    tb.skipWhitespace
    tb.length match {
      case 0 => TEmpty
      case _ =>
        val t: Type = tb.getChar match {
          case '_' => TEmpty
          case '?' => TAny
          case 'b' => TBool
          case 'i' => parseInt(tb)
          case 'w' => parseUInt(tb)
          case 's' => TStr
          case 't' => TTime
          case 'v' => TValue(parseUnits(tb))
          case 'c' => TComplex(parseUnits(tb))
          case '(' => parseCluster(tb)
          case '<' => parseChoice(tb)
          case '*' => parseArray(tb)
          case 'E' => parseError(tb)
          case char => error("Unknown character in type tag: " + char)
        }
        tb.skipWhitespace
        t
    }
  }

  private def parseInt(tb: Buffer): Type = {
    val bitWidth = parseNum(tb) getOrElse 32
    TInteger
  }
  
  private def parseUInt(tb: Buffer): Type = {
    val bitWidth = parseNum(tb) getOrElse 32
    TWord
  }
  
  private def parseNum(tb: Buffer): Option[Int] = {
    var n = 0
    var digits = 0
    while (!tb.isEmpty && tb.peekChar.isDigit) {
      n = 10*n + tb.get().toInt
      digits += 1
    }
    digits match {
      case 0 => None
      case _ => Some(n)
    }
  }
  
  private def parseCluster(tb: Buffer): Type = {
    val subTypes = Seq.newBuilder[Type]
    while (!tb.isEmpty) {
      if (tb.peekChar == ')') {
        tb.getChar
        return TCluster(subTypes.result: _*)
      } else if (tb.length >= 4 && tb.peek(4) == "...)") {
        tb.get(4)
        subTypes.result match {
          case Seq(t) => return TExpando(t)
          case _ => error("Expando cluster requires one element type.")
        }
      }
      subTypes += parseSingleType(tb)
    }
    error("No closing ) found.")
  }

  private def parseChoice(tb: Buffer): Type = {
    var currentType = Seq.newBuilder[Type]
    val subTypes = Seq.newBuilder[Type]
    while (!tb.isEmpty) {
      tb.peekChar match {
        case '|' =>
          tb.getChar
          subTypes += {
            currentType.result match {
              case Seq() => error("Choice elements cannot be empty clusters")
              case Seq(t) => t
              case types => TCluster(types: _*)
            }
          }
          currentType = Seq.newBuilder[Type]
        case '>' =>
          tb.getChar
          subTypes += {
            currentType.result match {
              case Seq() => error("Choice elements cannot be empty clusters")
              case Seq(t) => t
              case types => TCluster(types: _*)
            }
          }
          subTypes.result match {
            case Seq() => error("Choice must contain at least one type.")
            case subTypes => return TChoice(subTypes: _*)
          }
        case _ =>
          currentType += parseSingleType(tb)
      }
    }
    error("No closing '>' found.")
  }
  
  private def parseArray(tb: Buffer): Type = {
    tb.skipWhitespace
    var depth = 0
    var nDigits = 0
    while (tb.peekChar.isDigit) {
      depth = depth * 10 + tb.get().toInt
      nDigits += 1
    }
    if (depth == 0) {
      if (nDigits > 0) error("List depth must be non-zero.")
      depth = 1
    }
    tb.skipWhitespace
    val elem = parseSingleType(tb)
    TArr(elem, depth)
  }

  private def parseError(tb: Buffer): Type =
    TError(parseSingleType(tb))

  private def parseUnits(tb: Buffer): String = {
    tb.skipWhitespace
    val units = new StringBuffer
    if (tb.isEmpty || (tb.peekChar != '[')) return null
    tb.getChar // drop '['
    while (!tb.isEmpty) {
      val c = tb.getChar
      if (c == ']') return units.toString
      units.append(c)
    }
    error("No closing ] found.")
  }
}


case object TEmpty extends Type {
  def accepts(typ: Type) = typ == TEmpty

  def isAbstract = false
  def fixedWidth = true
  def dataWidth = 0
  
  override def toString = ""
}


case object TAny extends Type {
  def accepts(typ: Type) = true

  def isAbstract = true
  def fixedWidth = false
  def dataWidth = 0
  
  override def toString = "?"
}


case class TChoice(choices: Type*) extends Type {
  def accepts(typ: Type) = typ match {
    case TChoice(types @ _*) => types forall (accepts _)
    case typ => choices.exists(_ accepts typ)
  }
  
  override def expanded = choices.flatMap(_.expanded).toList
  
  def isAbstract = true
  def fixedWidth = false
  def dataWidth = 0
  
  override def toString = "<" + choices.mkString("|") + ">"
}


case class TExpando(elem: Type) extends Type {
  def accepts(typ: Type) = typ match {
    case TCluster(elems @ _*) => elems.forall(elem accepts _)
    case _ => false
  }
  
  override def expanded = List(TAny)
  
  def isAbstract = true
  def fixedWidth = false
  def dataWidth = 0
  
  override def toString = "(" + elem.toString + "...)"
}


case class TCluster(elems: Type*) extends Type {

  lazy val isAbstract = elems exists (_.isAbstract)
  lazy val dataWidth = elems map (_.dataWidth) sum
  lazy val fixedWidth = elems forall (_.fixedWidth)
  
  val offsets = {
    val widths = elems.map(_.dataWidth)
    var prev = 0
    for (w <- widths) yield {
      val ofs = prev
      prev += w
      ofs
    }
  }

  override lazy val toString = "(" + elems.map(_.toString).mkString + ")"

  def accepts(typ: Type) = typ match {
    case TCluster(others @ _*) if others.size == size =>
      (elems zip others) forall { case (elem, other) => elem accepts other }
    case _ => false
  }

  override def expanded = {
    def combinations[T](lists: List[List[T]]): List[List[T]] = lists match {
      case Nil => List(Nil)
      case heads :: rest =>
        val tails = combinations(rest)
        for (head <- heads; tail <- tails) yield head :: tail
    }
    
    for (combo <- combinations(elems.map(_.expanded).toList))
      yield TCluster(combo: _*)
  }
  
  def size = elems.size
  def subtype(i: Int) = elems(i)
  def apply(i: Int) = elems(i)
  def offset(i: Int) = offsets(i)
}

//object TCluster {
//  def unapplySeq(c: TCluster): Option[Seq[Type]] = Some(c.elems)
//}


case class TArr(elem: Type, depth: Int = 1) extends Type {
  def isAbstract = elem.isAbstract
  
  override val toString = {
    // FIXME this is a bit of a hack to get empty lists to flatten properly in some circumstances
    val elemStr = if (elem == TEmpty) "_" else elem.toString
    if (depth == 1) "*" + elemStr
    else "*" + depth + elemStr
  }
  
  def accepts(typ: Type) = typ match {
    case TArr(e, d) if d == depth => elem accepts e
    case _ => false
  }

  override def expanded = for (e <- elem.expanded) yield TArr(e)
  
  val fixedWidth = false
  val dataWidth = 4 * depth + 4

  def offset(index: Int) = index * elem.dataWidth
}


case class TError(payload: Type) extends Type {
  def accepts(typ: Type) = typ match {
    case TError(p) => payload accepts p
    case _ => false
  }

  val isAbstract = payload.isAbstract
  val fixedWidth = false
  val dataWidth = 4 + 4 + payload.dataWidth

  override val toString = "E" + payload.toString
}


case object TBool extends Type {
  def accepts(typ: Type) = typ == TBool

  val isAbstract = false
  val fixedWidth = true
  val dataWidth = 1

  override val toString = "b"
}


case object TInteger extends Type {
  def accepts(typ: Type) = typ == TInteger

  val isAbstract = false
  val fixedWidth = true
  val dataWidth = 4

  override val toString = "i"
}


case object TWord extends Type {
  def accepts(typ: Type) = typ == TWord

  val isAbstract = false
  val fixedWidth = true
  val dataWidth = 4

  override val toString = "w"
}


case object TStr extends Type {
  def accepts(typ: Type) = typ == TStr

  val isAbstract = false
  val fixedWidth = false
  val dataWidth = 4

  override val toString = "s"
}


case object TTime extends Type {
  def accepts(typ: Type) = typ == TTime

  val isAbstract = false
  val fixedWidth = true
  val dataWidth = 16

  override val toString = "t"
}


case class TValue(units: Option[String] = None) extends Type {
  val isAbstract = false
  val fixedWidth = true
  val dataWidth = 8

  def accepts(typ: Type) = typ match {
    case TValue(u) => (units, u) match {
      case (None, None) => true
      case (None, Some(u)) => true
      case (Some(units), None) => true
      case (Some(units), Some(u)) => units == u
    }
    case _ => false
  }

  override val toString = units match {
    case None => "v"
    case Some(units) => "v[" + units + "]"
  }
}

object TValue {
  def apply(units: String) = new TValue(Option(units))
}


case class TComplex(units: Option[String] = None) extends Type {
  def isAbstract = false
  def fixedWidth = true
  def dataWidth = 16

  def accepts(typ: Type) = typ match {
    case TComplex(u) => (units, u) match {
      case (None, None) => true
      case (None, Some(u)) => true
      case (Some(units), None) => true
      case (Some(units), Some(u)) => units == u
    }
    case _ => false
  }

  override lazy val toString = units match {
    case None => "c"
    case Some(units) => "c[" + units + "]"
  }
}

object TComplex {
  def apply(units: String) = new TComplex(Option(units))
}
