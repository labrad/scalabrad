package org.labrad
package data

import io.netty.buffer.{ByteBuf, Unpooled}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, Serializable}
import java.nio.ByteOrder
import java.nio.ByteOrder.{BIG_ENDIAN, LITTLE_ENDIAN}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Date
import org.joda.time.{DateTime, DateTimeZone}
import org.labrad.data.EndianAwareByteArray._
import org.labrad.errors.{LabradException, NonIndexableTypeException}
import org.labrad.types._
import scala.annotation.tailrec
import scala.collection.mutable.Buffer
import scala.util.Try

case class Complex(real: Double, imag: Double)

case class Context(high: Long, low: Long) {
  def toData = Cluster(UInt(high), UInt(low))
}

case class TimeStamp(seconds: Long, fraction: Long) {
  def toDateTime: DateTime = new DateTime(toDate)
  def toDate: Date = {
    val s = seconds - TimeStamp.DELTA_SECONDS
    val f = (fraction.toDouble / Long.MaxValue * 1000).toLong
    new Date(s * 1000 + f)
  }
}

object TimeStamp {
  // time
  // LabRAD measures time as seconds and fractions of a second since Jan 1, 1904 GMT.
  // The Java Date class measures time as milliseconds since Jan 1, 1970 GMT.
  // The difference between these two is 24107 days.
  //
  val DELTA_SECONDS = 24107L * 24L * 60L * 60L

  def apply(date: Date): TimeStamp = {
    val millis = date.getTime
    val seconds = millis / 1000 + DELTA_SECONDS
    var fraction = millis % 1000
    fraction = (fraction.toDouble / 1000 * Long.MaxValue).toLong
    new TimeStamp(seconds, fraction)
  }
  def apply(dateTime: DateTime): TimeStamp = apply(dateTime.toDate)
}

/**
 * The Data class encapsulates the data format used to communicate between
 * LabRAD servers and clients.  This data format is based on the
 * capabilities of LabVIEW, from National Instruments.  Each piece of LabRAD
 * data has a Type object which is specified by a String type tag.
 */
trait Data {

  def t: Type

  override def equals(other: Any): Boolean = other match {
    case other: Data => Data.isEqual(this, other)
    case _ => false
  }

  def ~==(other: Any): Boolean = other match {
    case other: Data => Data.approxEqual(this, other)
    case _ => false
  }

  private[data] def cast(newType: Type): Data
  private[data] def castError: Data = {
    t match {
      case TError(payload) => cast(TCluster(TInt, TStr, payload))
      case _ => sys.error(s"cannot cast data of type $t to error cluster")
    }
  }

  def toBytes(implicit outputOrder: ByteOrder): Array[Byte] = {
    val buf = Unpooled.buffer()
    flatten(buf, outputOrder)
    buf.toByteArray
  }

  def flatten(buf: ByteBuf, outputOrder: ByteOrder): Unit

  override def toString = t match {
    case TNone => "_"

    case TBool => getBool.toString

    case TInt =>
      getInt match {
        case i if i >= 0 => "+" + i
        case i => i.toString
      }

    case TUInt => getUInt.toString

    case TValue(u) =>
      getValue.toString + (u match {
        case Some("") | None => ""
        case Some(unit) => " " + unit
      })

    case TComplex(u) =>
      val c = getComplex
      c.real.toString + (if (c.imag >= 0) "+" else "") +
      c.imag.toString + "i" + (u match {
        case Some("") | None => ""
        case Some(unit) => " " + unit
      })

    case TTime => getTime.toDateTime.withZone(DateTimeZone.UTC).toString

    case TStr => Translate.toStringLiteral(getString)

    case TBytes => Translate.toBytesLiteral(getBytes)

    case TArr(elem, depth) =>
      val shape = arrayShape
      val idx = Array.ofDim[Int](depth)
      val sb = new StringBuilder
      val it = flatIterator
      def buildString(k: Int): Unit = {
        sb += '['
        for (i <- 0 until shape(k)) {
          idx(k) = i
          if (i > 0) sb ++= ", "
          if (k == shape.length - 1)
            sb ++= it.next().toString
          else
            buildString(k + 1)
        }
        sb += ']'
      }
      buildString(0)
      sb.toString

    case TCluster(elems @ _*) =>
      def isKeyType(t: Type): Boolean = (t == TStr || t == TInt || t == TUInt)
      val keyTypes = elems.map {
        case TCluster(keyType, valueType) if isKeyType(keyType) => Some(keyType)
        case _ => None
      }
      if (elems.size >= 1 && keyTypes.forall(_ != None) && keyTypes.flatten.toSet.size == 1) {
        // show as a map
        val items = clusterIterator.map {
          case Cluster(key, value) => s"$key: $value"
        }
        "{" + items.mkString(", ") + "}"
      } else {
        // show as a regular cluster
        "(" + clusterIterator.mkString(", ") + ")"
      }

    case TError(_) =>
      s"Error($getErrorCode, $getErrorMessage, $getErrorPayload)"
  }

  def toPrettyString(width: Int = 80): String = toDoc.pretty(width)

  def toDoc: Pretty.Doc = {
    import Pretty._
    t match {
      case TNone => text(this.toString)
      case TBool => text(this.toString)
      case TInt => text(this.toString)
      case TUInt => text(this.toString)
      case TValue(u) => text(this.toString)
      case TComplex(u) => text(this.toString)
      case TTime => text(this.toString)
      case TStr => text(this.toString)
      case TBytes => text(this.toString)
      case TArr(elem, depth) =>
        val shape = arrayShape
        def buildDoc(idx: Seq[Int]): Doc = {
          val k = idx.length
          val items = if (k == shape.length - 1) {
            // array elements converted to docs
            LazyList.tabulate(shape(k)) { i => this((idx :+ i): _*).toDoc }
          } else {
            // subarrays converted to docs
            LazyList.tabulate(shape(k)) { i => buildDoc((idx :+ i)) }
          }
          bracketAll("[", items, "]")
        }
        buildDoc(Seq())

      case TCluster(elems @ _*) =>
        def isKeyType(t: Type): Boolean = (t == TStr || t == TInt || t == TUInt)
        val keyTypes = elems.map {
          case TCluster(keyType, valueType) if isKeyType(keyType) => Some(keyType)
          case _ => None
        }
        if (elems.size >= 1 && keyTypes.forall(_ != None) && keyTypes.flatten.toSet.size == 1) {
          // show as a map
          val items = LazyList.from(clusterIterator).map {
            case Cluster(key, value) => text(key.toString) <> text(": ") <> value.toDoc
          }
          bracketAll("{", items, "}")
        } else {
          // show as a regular cluster
          val elems = LazyList.from(clusterIterator).map { _.toDoc }
          bracketAll("(", elems, ")")
        }

      case TError(_) =>
        val items = LazyList(
          text(getErrorCode.toString),
          text(getErrorMessage.toString),
          getErrorPayload.toDoc
        )
        bracketAll("Error(", items, ")")
    }
  }

  def convertTo(pattern: String): Data
  def convertTo(pattern: Pattern): Data

  // type checks
  def isNone = t == TNone
  def isBool = t == TBool
  def isInt = t == TInt
  def isUInt = t == TUInt
  def isBytes = t == TBytes
  def isString = t == TStr
  def isValue = t.isInstanceOf[TValue]
  def isComplex = t.isInstanceOf[TComplex]
  def isTime = t == TTime
  def isArray = t.isInstanceOf[TArr]
  def isCluster = t.isInstanceOf[TCluster]
  def isError = t.isInstanceOf[TError]
  def hasUnits = t match {
    case TValue(Some(_)) | TComplex(Some(_)) => true
    case TValue(None) | TComplex(None) => false
    case _ => false
  }

  /**
   * Get a Data subobject at the specified array of indices.  Note that
   * this returns a view rather than a copy, so any modifications to
   * the subobject will be reflected in the original data.
   */
  def apply(idx: Int*): Data

  /** Return an iterator that runs over all the elements in an N-dimensional array */
  def flatIterator: Iterator[Data]

  // structures
  def arraySize: Int
  def arrayShape: Array[Int]
  def arrayBytes: EndianAwareByteSlice

  def setArrayShape(shape: Array[Int]): Unit
  def setArrayShape(shape: Int*): Unit = setArrayShape(shape.toArray)
  def setArraySize(size: Int): Unit = setArrayShape(size)

  def clusterSize: Int
  def clusterIterator: Iterator[Data]

  // getters
  def getBool: Boolean
  def getInt: Int
  def getUInt: Long
  def getBytes: Array[Byte]
  def getString: String
  def getValue: Double
  def getReal: Double
  def getImag: Double
  def getComplex: Complex
  def getTime: TimeStamp
  def getSeconds: Long
  def getFraction: Long

  def getErrorCode: Int
  def getErrorMessage: String
  def getErrorPayload: Data

  def get[T](implicit getter: Getter[T]): T = getter.get(this)

  // setters

  def set(other: Data): Unit = { Data.copy(other, this) }
  def set[T](value: T)(implicit setter: Setter[T]): Unit = { setter.set(this, value) }

  def setBool(b: Boolean): Unit

  def setInt(i: Int): Unit

  def setUInt(u: Long): Unit

  def setBytes(bytes: Array[Byte]): Unit

  def setString(s: String): Unit

  def setValue(d: Double): Unit

  def setComplex(re: Double, im: Double): Unit
  def setComplex(c: Complex): Unit = setComplex(c.real, c.imag)

  def setTime(seconds: Long, fraction: Long): Unit
  def setTime(timestamp: TimeStamp): Unit = setTime(timestamp.seconds, timestamp.fraction)
  def setTime(date: Date): Unit = setTime(TimeStamp(date))
  def setTime(dateTime: DateTime): Unit = setTime(TimeStamp(dateTime))

  def setError(code: Int, message: String, payload: Data = Data.NONE): Unit = t match {
    case TError(payloadType) =>
      val data = castError
      val it = data.clusterIterator
      it.next().setInt(code)
      it.next().setString(message)
      it.next().set(payload)
    case _ => sys.error("Data type must be error")
  }
}

class Cursor(var t: Type, val buf: Array[Byte], var ofs: Int)(implicit byteOrder: ByteOrder) {

  def toData: FlatData = new FlatData(t, buf, ofs)

  def len: Int = {
    t match {
      case TNone | TBool | TInt | TUInt | TValue(_) | TComplex(_) | TTime =>
        t.dataWidth

      case TStr | TBytes =>
        val len = buf.getInt(ofs)
        4 + len

      case TArr(elem, depth) =>
        val size = _arrayShape(depth).product
        var len = 4 * depth
        if (elem.fixedWidth) {
          len += size * elem.dataWidth
        } else {
          val iter = _arrayCursor(elem, depth)
          while (iter.hasNext) {
            len += iter.next().len
          }
        }
        len

      case t: TCluster =>
        var len = 0
        if (t.fixedWidth) {
          len += t.dataWidth
        } else {
          val iter = new ClusterCursor(t.elems, buf, ofs)
          while (iter.hasNext) {
            len += iter.next().len
          }
        }
        len

      case TError(payload) =>
        _errorCursor.len
    }
  }

  def flatten(os: ByteBuf, outputOrder: ByteOrder): Unit = {
    if (outputOrder == byteOrder)
      os.writeBytes(buf, ofs, len)
    else {
      t match {
        case TNone =>

        case TBool =>
          os.writeBoolean(buf.getBool(ofs))

        case TInt =>
          os.writeIntOrdered(buf.getInt(ofs))(outputOrder)

        case TUInt =>
          os.writeIntOrdered(buf.getUInt(ofs).toInt)(outputOrder)

        case TValue(_) =>
          os.writeDoubleOrdered(buf.getDouble(ofs))(outputOrder)

        case TComplex(_) =>
          os.writeDoubleOrdered(buf.getDouble(ofs))(outputOrder)
          os.writeDoubleOrdered(buf.getDouble(ofs + 8))(outputOrder)

        case TTime =>
          os.writeLongOrdered(buf.getLong(ofs))(outputOrder)
          os.writeLongOrdered(buf.getLong(ofs + 8))(outputOrder)

        case TStr | TBytes =>
          val len = buf.getInt(ofs)
          os.writeIntOrdered(len)(outputOrder)
          os.writeBytes(buf, ofs + 4, len)

        case TArr(elem, depth) =>
          // write arr shape and compute total number of elements in the list
          var size = 1
          for (i <- 0 until depth) {
            val dim = buf.getInt(ofs + 4*i)
            os.writeIntOrdered(dim)(outputOrder)
            size *= dim
          }

          // write arr data
          if (elem.fixedWidth && outputOrder == byteOrder) {
            // for fixed-width data, just copy in one big chunk
            os.writeBytes(buf, ofs + 4*depth, elem.dataWidth * size)
          } else {
            // for variable-width data, flatten recursively
            val iter = _arrayCursor(elem, depth)
            while (iter.hasNext) {
              iter.next().flatten(os, outputOrder)
            }
          }

        case t: TCluster =>
          val iter = _clusterCursor(t)
          while (iter.hasNext) {
            iter.next().flatten(os, outputOrder)
          }

        case TError(payload) =>
          _errorCursor.flatten(os, outputOrder)
      }
    }
  }

  private def _errorCursor: Cursor = {
    t match {
      case TError(payload) => new Cursor(TCluster(TInt, TStr, payload), buf, ofs)
      case t => sys.error(s"cannot create error cursor for data of type $t")
    }
  }

  private def _arrayShape(depth: Int): Array[Int] = {
    Array.tabulate(depth) { i => buf.getInt(ofs + 4*i) }
  }

  private def _arrayCursor(elem: Type, depth: Int): ArrayCursor = {
    val size = _arrayShape(depth).product
    new ArrayCursor(elem, buf, size, ofs + 4 * depth)
  }

  private def _clusterCursor(t: TCluster): ClusterCursor = {
    new ClusterCursor(t.elems, buf, ofs)
  }
}

class ArrayCursor(val t: Type, val buf: Array[Byte], size: Int, ofs: Int)(implicit byteOrder: ByteOrder) extends Iterator[Cursor] {

  var idx = 0
  var elemOfs = ofs

  val cursor = new Cursor(t, buf, ofs)

  def hasNext: Boolean = {
    idx < size
  }

  def next(): Cursor = {
    require(hasNext)
    cursor.ofs = elemOfs
    idx += 1
    elemOfs += cursor.len
    cursor
  }
}

class ClusterCursor(ts: Seq[Type], buf: Array[Byte], ofs: Int)(implicit byteOrder: ByteOrder) extends Iterator[Cursor] {

  var idx = 0
  var elemOfs = ofs

  val cursor = new Cursor(TNone, buf, ofs)

  def hasNext: Boolean = {
    idx < ts.size
  }

  def next(): Cursor = {
    require(hasNext)
    cursor.t = ts(idx)
    cursor.ofs = elemOfs
    idx += 1
    elemOfs += cursor.len
    cursor
  }

}


/**
 * The Data class encapsulates the data format used to communicate between
 * LabRAD servers and clients.  This data format is based on the
 * capabilities of LabVIEW, from National Instruments.  Each piece of LabRAD
 * data has a Type object which is specified by a String type tag.
 */
class FlatData private[data] (val t: Type, buf: Array[Byte], ofs: Int)(implicit byteOrder: ByteOrder)
extends Data with Serializable with Cloneable {

  override def clone = {
    val newBytes = buf.slice(ofs, ofs + len)
    new FlatData(t, buf, ofs)
  }

  def cursor: Cursor = new Cursor(t, buf, ofs)

  lazy val len: Int = cursor.len

  /**
   * Create a new Data object that reinterprets the current data with a new type.
   */
  override private[data] def cast(newType: Type) = new FlatData(newType, buf, ofs)
  override private[data] def castError: FlatData = {
    t match {
      case TError(payload) => cast(TCluster(TInt, TStr, payload))
      case _ => sys.error(s"cannot cast data of type $t to error cluster")
    }
  }

  def flatten(out: ByteBuf, outputOrder: ByteOrder): Unit = {
    cursor.flatten(out, outputOrder)
  }

  def convertTo(pattern: String): Data = convertTo(Pattern(pattern))
  def convertTo(pattern: Pattern): Data = {
    // XXX: converting empty lists is a special case.
    // If we are converting to a list with the same number of dimensions and
    // the target element type is a concrete type, we allow this conversion.
    // We test whether the pattern is a concrete type by parsing it as a type.
    val empty = (t, pattern) match {
      case (TArr(TNone, depth), PArr(pat, pDepth)) if depth == pDepth && arrayShape.product == 0 =>
        val elemType = Try(Type(pat.toString)).toOption
        elemType.map(t => cast(TArr(t, depth)))
      case _ =>
        None
    }

    empty.getOrElse {
      pattern(t) match {
        case Some(tgt) =>
          Data.makeConverter(t, tgt)(this)
          cast(tgt)
        case None =>
          sys.error(s"cannot convert data from '$t' to '$pattern'")
      }
    }
  }

  /**
   * Get a Data subobject at the specified array of indices.  Note that
   * this returns a view rather than a copy, so any modifications to
   * the subobject will be reflected in the original data.
   */
  def apply(idx: Int*): Data = {
    if (idx.isEmpty) {
      this
    } else {
      t match {
        case TArr(elem, depth) =>
          require(idx.size >= depth, "not enough indices for array")
          val arrIdx = Data.flatIndex(arrayShape, idx)
          if (elem.fixedWidth) {
            new FlatData(elem, buf, ofs + 4 * depth + arrIdx * elem.dataWidth)
          } else {
            flatIterator.drop(arrIdx).next()(idx.drop(depth): _*)
          }

        case t: TCluster =>
          clusterIterator.drop(idx.head).next()(idx.tail: _*)

        case _ =>
          sys.error(s"type $t is not indexable")
      }
    }
  }

  /** Return an iterator that runs over all the elements in an N-dimensional array */
  def flatIterator: Iterator[FlatData] = t match {
    case TArr(elem, depth) =>
      val size = arrayShape.product
      val iter = new ArrayCursor(elem, buf, size, ofs + 4 * depth)
      iter.map(_.toData)
    case _ =>
      sys.error("can only flat iterate over arrays")
  }

  // structures
  def arraySize = t match {
    case TArr(_, 1) => buf.getInt(ofs)
    case _ => sys.error("arraySize is only defined for 1D arrays")
  }

  def arrayShape = t match {
    case TArr(_, depth) => Array.tabulate(depth) { i => buf.getInt(ofs + 4*i) }
    case _ => sys.error("arrayShape is only defined for arrays")
  }

  def arrayBytes: EndianAwareByteSlice = {
    t match {
      case TArr(elem, depth) =>
        require(elem.fixedWidth, s"arrayBytes only allowed for fixed-width elem types: $elem")
        val size = arrayShape.product
        EndianAwareByteSlice(buf, ofs + 4 * depth, elem.dataWidth * size)
      case _ =>
        sys.error("arrayBytes is only defined for arrays")
    }
  }

  def setArrayShape(shape: Array[Int]): Unit = t match {
    case TArr(elem, depth) =>
      require(shape.length == depth)
      sys.error("cannot set array shape of flat data")
    case _ =>
      sys.error("arrayShape can only be set for arrays")
  }

  def clusterSize: Int = t match {
    case TCluster(elems @ _*) => elems.size
    case _ => sys.error("clusterSize is only defined for clusters")
  }

  def clusterIterator: Iterator[FlatData] = t match {
    case t @ TCluster(elems @ _*) =>
      val iter = new ClusterCursor(elems, buf, ofs)
      iter.map(_.toData)
    case _ =>
      sys.error("can only cluster iterate over clusters")
  }

  // getters
  def getBool: Boolean = { require(isBool); buf.getBool(ofs) }
  def getInt: Int = { require(isInt); buf.getInt(ofs) }
  def getUInt: Long = { require(isUInt); buf.getUInt(ofs) }
  def getBytes: Array[Byte] = {
    // TODO: when bytes/string separation is complete, require isBytes only
    require(isBytes || isString)
    val len = buf.getInt(ofs)
    buf.slice(ofs + 4, ofs + 4 + len)
  }
  def getString: String = {
    require(isString)
    val len = buf.getInt(ofs)
    new String(buf.slice(ofs + 4, ofs + 4 + len), UTF_8)
  }
  def getValue: Double = { require(isValue); buf.getDouble(ofs) }
  def getReal: Double = { require(isComplex); buf.getDouble(ofs) }
  def getImag: Double = { require(isComplex); buf.getDouble(ofs + 8) }
  def getComplex: Complex = { require(isComplex); Complex(buf.getDouble(ofs), buf.getDouble(ofs + 8)) }
  def getTime: TimeStamp = { require(isTime); TimeStamp(buf.getLong(ofs), buf.getLong(ofs + 8)) }
  def getSeconds: Long = { require(isTime); buf.getLong(ofs) }
  def getFraction: Long = { require(isTime); buf.getLong(ofs + 8) }

  def getErrorCode = { require(isError); buf.getInt(ofs) }
  def getErrorMessage = { require(isError); val len = buf.getInt(ofs + 4); new String(buf.slice(ofs + 8, ofs + 8 + len), UTF_8) }
  def getErrorPayload = t match {
    case TError(payload) => castError(2)
    case _ => sys.error("errorPayload is only defined for errors")
  }

  // setters

  def setBool(b: Boolean): Unit = {
    require(isBool)
    buf.setBool(ofs, b)
  }

  def setInt(i: Int): Unit = {
    require(isInt)
    buf.setInt(ofs, i)
  }

  def setUInt(u: Long): Unit = {
    require(isUInt)
    buf.setUInt(ofs, u)
  }

  def setBytes(bytes: Array[Byte]): Unit = {
    // TODO: when bytes/string separation is complete, require isBytes only
    require(isBytes || isString)
    sys.error("unable to set bytes on flat data")
  }

  def setString(s: String): Unit = {
    require(isString)
    sys.error("unable to set string on flat data")
  }

  def setValue(d: Double): Unit = {
    require(isValue)
    buf.setDouble(ofs, d)
  }

  def setComplex(re: Double, im: Double): Unit = {
    require(isComplex)
    buf.setDouble(ofs, re)
    buf.setDouble(ofs + 8, im)
  }

  def setTime(seconds: Long, fraction: Long): Unit = {
    require(isTime)
    buf.setLong(ofs, seconds)
    buf.setLong(ofs + 8, fraction)
  }
}

object FlatData {
  def fromBytes(t: Type, buf: Array[Byte])(implicit bo: ByteOrder): Data = {
    val data = new FlatData(t, buf, 0)
    assert(data.len == buf.length)
    data
  }

  def fromBytes(t: Type, in: ByteBuf)(implicit bo: ByteOrder): Data = {
    def traverse(t: Type, ofs: Int): Int = {
      if (t.fixedWidth) {
        t.dataWidth
      } else {
        t match {
          case TStr | TBytes =>
            val len = in.getIntOrdered(ofs)
            4 + len

          case TArr(elem, depth) =>
            var size = 1
            for (i <- 0 until depth) {
              val dim = in.getIntOrdered(ofs + 4 * i)
              size *= dim
            }
            var len = 0
            for (i <- 0 until size) {
              len += traverse(elem, ofs + 4 * depth + len)
            }
            len

          case t: TCluster =>
            var len = 0
            for (elem <- t.elems) {
              len += traverse(elem, ofs + len)
            }
            len

          case TError(payload) =>
            traverse(TCluster(TInt, TStr, payload), ofs)

          case _ =>
            sys.error(s"missing case to handle non-fixed-width type: $t")
        }
      }
    }
    val len = traverse(t, in.readerIndex)
    val buf = Array.ofDim[Byte](len)
    in.readBytes(len)
    new FlatData(t, buf, 0)
  }
}

/**
 * The Data class encapsulates the data format used to communicate between
 * LabRAD servers and clients.  This data format is based on the
 * capabilities of LabVIEW, from National Instruments.  Each piece of LabRAD
 * data has a Type object which is specified by a String type tag.
 */
class TreeData private[data] (val t: Type, buf: Array[Byte], ofs: Int, heap: Buffer[Array[Byte]])(implicit byteOrder: ByteOrder)
extends Data with Serializable with Cloneable {

  override def clone = Data.copy(this, Data(this.t))

  /**
   * Create a new Data object that reinterprets the current data with a new type.
   */
  override private[data] def cast(newType: Type) = new TreeData(newType, buf, ofs, heap)

  def flatten(out: ByteBuf, outputOrder: ByteOrder): Unit = flatten(out, t, buf, ofs, outputOrder)

  private def flatten(os: ByteBuf, t: Type, buf: Array[Byte], ofs: Int, outputOrder: ByteOrder): Unit = {
    if (t.fixedWidth && outputOrder == byteOrder)
      os.writeBytes(buf, ofs, t.dataWidth)
    else {
      t match {
        case TNone =>

        case TBool =>
          os.writeBoolean(buf.getBool(ofs))

        case TInt =>
          os.writeIntOrdered(buf.getInt(ofs))(outputOrder)

        case TUInt =>
          os.writeIntOrdered(buf.getUInt(ofs).toInt)(outputOrder)

        case TValue(_) =>
          os.writeDoubleOrdered(buf.getDouble(ofs))(outputOrder)

        case TComplex(_) =>
          os.writeDoubleOrdered(buf.getDouble(ofs))(outputOrder)
          os.writeDoubleOrdered(buf.getDouble(ofs + 8))(outputOrder)

        case TTime =>
          os.writeLongOrdered(buf.getLong(ofs))(outputOrder)
          os.writeLongOrdered(buf.getLong(ofs + 8))(outputOrder)

        case TStr | TBytes =>
          val strBuf = heap(buf.getInt(ofs))
          os.writeIntOrdered(strBuf.length)(outputOrder)
          os.writeBytes(strBuf)

        case TArr(elem, depth) =>
          // write arr shape and compute total number of elements in the list
          var size = 1
          for (i <- 0 until depth) {
            val dim = buf.getInt(ofs + 4*i)
            os.writeIntOrdered(dim)(outputOrder)
            size *= dim
          }

          // write arr data
          val arrBuf = heap(buf.getInt(ofs + 4*depth))
          if (elem.fixedWidth && outputOrder == byteOrder) {
            // for fixed-width data, just copy in one big chunk
            os.writeBytes(arrBuf, 0, elem.dataWidth * size)
          } else {
            // for variable-width data, flatten recursively
            for (i <- 0 until size)
              flatten(os, elem, arrBuf, elem.dataWidth * i, outputOrder)
          }

        case t: TCluster =>
          for ((elem, delta) <- t.elems zip t.offsets)
            flatten(os, elem, buf, ofs + delta, outputOrder)

        case TError(payload) =>
          flatten(os, TCluster(TInt, TStr, payload), buf, ofs, outputOrder)
      }
    }
  }

  def convertTo(pattern: String): Data = convertTo(Pattern(pattern))
  def convertTo(pattern: Pattern): Data = {
    // XXX: converting empty lists is a special case.
    // If we are converting to a list with the same number of dimensions and
    // the target element type is a concrete type, we allow this conversion.
    // We test whether the pattern is a concrete type by parsing it as a type.
    val empty = (t, pattern) match {
      case (TArr(TNone, depth), PArr(pat, pDepth)) if depth == pDepth && arrayShape.product == 0 =>
        val elemType = Try(Type(pat.toString)).toOption
        elemType.map(t => cast(TArr(t, depth)))
      case _ =>
        None
    }

    empty.getOrElse {
      pattern(t) match {
        case Some(tgt) =>
          Data.makeConverter(t, tgt)(this)
          cast(tgt)
        case None =>
          sys.error(s"cannot convert data from '$t' to '$pattern'")
      }
    }
  }

  /**
   * Get a Data subobject at the specified array of indices.  Note that
   * this returns a view rather than a copy, so any modifications to
   * the subobject will be reflected in the original data.
   */
  def apply(idx: Int*): Data = {
    @tailrec
    def find(t: Type, buf: Array[Byte], ofs: Int, idx: Seq[Int]): Data =
      if (idx.isEmpty)
        new TreeData(t, buf, ofs, heap)
      else
        t match {
          case TArr(elem, depth) =>
            require(idx.size >= depth, "not enough indices for array")
            val arrBuf = heap(buf.getInt(ofs + 4 * depth))
            val arrIdx = Data.flatIndex(arrayShape, idx)
            find(elem, arrBuf, arrIdx * elem.dataWidth, idx.drop(depth))

          case t: TCluster =>
            find(t.elems(idx.head), buf, ofs + t.offsets(idx.head), idx.tail)

          case _ =>
            sys.error(s"type $t is not indexable")
        }
    find(this.t, this.buf, this.ofs, idx)
  }

  /** Return an iterator that runs over all the elements in an N-dimensional array */
  def flatIterator: Iterator[Data] = t match {
    case TArr(elem, depth) =>
      val size = arrayShape.product
      val arrBuf = heap(buf.getInt(ofs + 4 * depth))
      Iterator.tabulate(size) { i =>
        new TreeData(elem, arrBuf, i * elem.dataWidth, heap)
      }
    case _ =>
      sys.error("can only flat iterate over arrays")
  }

  // structures
  def arraySize = t match {
    case TArr(_, 1) => buf.getInt(ofs)
    case _ => sys.error("arraySize is only defined for 1D arrays")
  }

  def arrayShape = t match {
    case TArr(_, depth) => Array.tabulate(depth) { i => buf.getInt(ofs + 4*i) }
    case _ => sys.error("arrayShape is only defined for arrays")
  }

  def arrayBytes: EndianAwareByteSlice = {
    t match {
      case TArr(elem, depth) =>
        require(elem.fixedWidth, s"arrayBytes only allowed for fixed-width elem types: $elem")
        val size = arrayShape.product
        val arrBuf = heap(buf.getInt(ofs + 4 * depth))
        EndianAwareByteSlice(arrBuf, 0, elem.dataWidth * size)
      case _ =>
        sys.error("arrayBytes is only defined for arrays")
    }
  }

  def setArrayShape(shape: Array[Int]): Unit = t match {
    case TArr(elem, depth) =>
      require(shape.length == depth)
      val size = shape.product
      for (i <- 0 until depth)
        buf.setInt(ofs + 4*i, shape(i))
      val newBuf = TreeData.newByteArray(elem.dataWidth * size)
      val heapIndex = buf.getInt(ofs + 4*depth)
      if (heapIndex == -1) {
        buf.setInt(ofs + 4*depth, heap.size)
        heap += newBuf
      } else {
        val oldBuf = heap(heapIndex)
        Array.copy(oldBuf, 0, newBuf, 0, oldBuf.size min newBuf.size)
        heap(heapIndex) = newBuf
      }
    case _ =>
      sys.error("arrayShape can only be set for arrays")
  }

  def clusterSize: Int = t match {
    case TCluster(elems @ _*) => elems.size
    case _ => sys.error("clusterSize is only defined for clusters")
  }

  def clusterIterator: Iterator[Data] = t match {
    case t @ TCluster(elems @ _*) =>
      val size = clusterSize
      var ofs = this.ofs
      elems.iterator.map { elem =>
        val result = new TreeData(elem, buf, ofs, heap)
        ofs += elem.dataWidth
        result
      }
    case _ =>
      sys.error("can only cluster iterate over clusters")
  }

  // getters
  def getBool: Boolean = { require(isBool); buf.getBool(ofs) }
  def getInt: Int = { require(isInt); buf.getInt(ofs) }
  def getUInt: Long = { require(isUInt); buf.getUInt(ofs) }
  def getBytes: Array[Byte] = {
    // TODO: when bytes/string separation is complete, require isBytes only
    require(isBytes || isString)
    heap(buf.getInt(ofs))
  }
  def getString: String = {
    require(isString)
    new String(heap(buf.getInt(ofs)), UTF_8)
  }
  def getValue: Double = { require(isValue); buf.getDouble(ofs) }
  def getReal: Double = { require(isComplex); buf.getDouble(ofs) }
  def getImag: Double = { require(isComplex); buf.getDouble(ofs + 8) }
  def getComplex: Complex = { require(isComplex); Complex(buf.getDouble(ofs), buf.getDouble(ofs + 8)) }
  def getTime: TimeStamp = { require(isTime); TimeStamp(buf.getLong(ofs), buf.getLong(ofs + 8)) }
  def getSeconds: Long = { require(isTime); buf.getLong(ofs) }
  def getFraction: Long = { require(isTime); buf.getLong(ofs + 8) }

  def getErrorCode = { require(isError); buf.getInt(ofs) }
  def getErrorMessage = { require(isError); new String(heap(buf.getInt(ofs + 4)), UTF_8) }
  def getErrorPayload = t match {
    case TError(payload) => new TreeData(payload, buf, ofs + 8, heap)
    case _ => sys.error("errorPayload is only defined for errors")
  }

  // setters

  def setBool(b: Boolean): Unit = {
    require(isBool)
    buf.setBool(ofs, b)
  }

  def setInt(i: Int): Unit = {
    require(isInt)
    buf.setInt(ofs, i)
  }

  def setUInt(u: Long): Unit = {
    require(isUInt)
    buf.setUInt(ofs, u)
  }

  def setBytes(bytes: Array[Byte]): Unit = {
    // TODO: when bytes/string separation is complete, require isBytes only
    require(isBytes || isString)
    var heapIndex = buf.getInt(ofs)
    if (heapIndex == -1) {
      // not yet set in the heap
      buf.setInt(ofs, heap.size)
      heap += bytes
    } else {
      // already set in the heap, reuse old spot
      heap(heapIndex) = bytes
    }
  }

  def setString(s: String): Unit = {
    require(isString)
    val bytes = s.getBytes(UTF_8)
    var heapIndex = buf.getInt(ofs)
    if (heapIndex == -1) {
      // not yet set in the heap
      buf.setInt(ofs, heap.size)
      heap += bytes
    } else {
      // already set in the heap, reuse old spot
      heap(heapIndex) = bytes
    }
  }

  def setValue(d: Double): Unit = {
    require(isValue)
    buf.setDouble(ofs, d)
  }

  def setComplex(re: Double, im: Double): Unit = {
    require(isComplex)
    buf.setDouble(ofs, re)
    buf.setDouble(ofs + 8, im)
  }

  def setTime(seconds: Long, fraction: Long): Unit = {
    require(isTime)
    buf.setLong(ofs, seconds)
    buf.setLong(ofs + 8, fraction)
  }
}

object TreeData {
  def apply(tag: String): TreeData = apply(Type(tag))
  def apply(t: Type)(implicit bo: ByteOrder = BIG_ENDIAN): TreeData =
    new TreeData(t, newByteArray(t.dataWidth), 0, newHeap)

  private[data] def newByteArray(length: Int): Array[Byte] = Array.fill[Byte](length)(0xFF.toByte)
  private[data] def newHeap: Buffer[Array[Byte]] = Buffer.empty[Array[Byte]]

  def fromBytes(t: Type, buf: Array[Byte])(implicit bo: ByteOrder): Data = {
    val in = Unpooled.wrappedBuffer(buf)
    val data = fromBytes(t, in)
    assert(in.readableBytes == 0, "not all bytes consumed when unflattening from array")
    data
  }

  def fromBytes(t: Type, in: ByteBuf)(implicit bo: ByteOrder): Data = {
    val buf = Array.ofDim[Byte](t.dataWidth)
    val heap = newHeap
    def unflatten(t: Type, buf: Array[Byte], ofs: Int): Unit = {
      if (t.fixedWidth)
        in.readBytes(buf, ofs, t.dataWidth)
      else
        t match {
          case TStr | TBytes =>
            val len = in.readIntOrdered()
            val strBuf = Array.ofDim[Byte](len)
            buf.setInt(ofs, heap.size)
            heap += strBuf
            in.readBytes(strBuf, 0, len)

          case TArr(elem, depth) =>
            var size = 1
            for (i <- 0 until depth) {
              val dim = in.readIntOrdered()
              buf.setInt(ofs + 4 * i, dim)
              size *= dim
            }
            val arrBuf = Array.ofDim[Byte](elem.dataWidth * size)
            buf.setInt(ofs + 4 * depth, heap.size)
            heap += arrBuf
            if (elem.fixedWidth)
              in.readBytes(arrBuf, 0, elem.dataWidth * size)
            else
              for (i <- 0 until size)
                unflatten(elem, arrBuf, elem.dataWidth * i)

          case t: TCluster =>
            for ((elem, delta) <- t.elems zip t.offsets)
              unflatten(elem, buf, ofs + delta)

          case TError(payload) =>
            unflatten(TCluster(TInt, TStr, payload), buf, ofs)

          case _ =>
            sys.error(s"missing case to handle non-fixed-width type: $t")
        }
    }
    unflatten(t, buf, 0)
    new TreeData(t, buf, 0, heap)
  }
}

object Data {
  val NONE = Data("")

  def apply(tag: String): Data = TreeData(tag)
  def apply(t: Type)(implicit bo: ByteOrder = BIG_ENDIAN): Data = TreeData(t)

  /**
   * Parse data from string representation
   */
  def parse(data: String): Data = Parsers.parseData(data)

  def copy(src: Data, dst: Data): Data = {
    require(src.t == dst.t, s"source and destination types do not match. src=${src.t}, dst=${dst.t}")
    src.t match {
      case TNone =>
      case TBool => dst.setBool(src.getBool)
      case TInt => dst.setInt(src.getInt)
      case TUInt => dst.setUInt(src.getUInt)
      case TValue(_) => dst.setValue(src.getValue)
      case TComplex(_) => dst.setComplex(src.getReal, src.getImag)
      case TTime => dst.setTime(src.getTime)
      case TStr => dst.setString(src.getString)
      case TBytes => dst.setBytes(src.getBytes)

      case TArr(elem, depth) =>
        val shape = src.arrayShape
        dst.setArrayShape(shape)
        for ((srcElem, dstElem) <- src.flatIterator zip dst.flatIterator) {
          copy(srcElem, dstElem)
        }

      case TCluster(elems @ _*) =>
        for ((s, d) <- src.clusterIterator zip dst.clusterIterator) {
          copy(s, d)
        }

      case TError(_) =>
        dst.setError(src.getErrorCode, src.getErrorMessage, src.getErrorPayload)
    }
    dst
  }


  // unflattening from bytes
  def fromBytes(t: Type, buf: Array[Byte])(implicit bo: ByteOrder): Data = {
    TreeData.fromBytes(t, buf)
  }

  def fromBytes(t: Type, in: ByteBuf)(implicit bo: ByteOrder): Data = {
    TreeData.fromBytes(t, in)
  }

  // equality testing
  def isEqual(a: Data, b: Data): Boolean = {
    if (a.t != b.t) {
      false
    } else {
      a.t match {
        case TNone => true
        case TBool => a.getBool == b.getBool
        case TInt => a.getInt == b.getInt
        case TUInt => a.getUInt == b.getUInt
        case _: TValue => a.getValue == b.getValue
        case _: TComplex => a.getReal == b.getReal && a.getImag == b.getImag
        case TTime => a.getSeconds == b.getSeconds && a.getFraction == b.getFraction
        case TStr => a.getString == b.getString
        case TBytes => a.getBytes.toSeq == b.getBytes.toSeq
        case _: TArr =>
          a.arrayShape.toSeq == b.arrayShape.toSeq &&
            (a.flatIterator zip b.flatIterator).forall { case (a, b) => a == b }
        case _: TCluster =>
          a.clusterSize == b.clusterSize &&
            (a.clusterIterator zip b.clusterIterator).forall { case (a, b) => a == b }
        case _: TError =>
          a.getErrorCode == b.getErrorCode && a.getErrorMessage == b.getErrorMessage && a.getErrorPayload == b.getErrorPayload
      }
    }
  }

  def approxEqual(a: Data, b: Data): Boolean = {
    if (a.t != b.t) {
      false
    } else {
      a.t match {
        case _: TValue => approxEqual(a.getValue, b.getValue)
        case _: TComplex => approxEqual(a.getReal, b.getReal) && approxEqual(a.getImag, b.getImag)
        case TTime => math.abs(a.getTime.toDate.getTime - b.getTime.toDate.getTime) <= 1
        case _: TArr =>
          a.arrayShape.toSeq == b.arrayShape.toSeq &&
            (a.flatIterator zip b.flatIterator).forall { case (a, b) => a ~== b }
        case TCluster(_*) =>
          a.clusterSize == b.clusterSize &&
            (a.clusterIterator zip b.clusterIterator).forall { case (a, b) => a ~== b }
        case _ => a == b
      }
    }
  }

  private def approxEqual(a: Double, b: Double, tol: Double = 1e-9): Boolean = {
    math.abs(a-b) < 5 * tol * math.abs(a+b) || math.abs(a-b) < Double.MinPositiveValue
  }

  def makeConverter(src: Type, tgt: Type): Data => Unit = {
    if (src == tgt) {
      data => ()
    } else {
      (src, tgt) match {
        case (TCluster(srcs @ _*), TCluster(tgts @ _*)) =>
          val funcs = (srcs zip tgts) map {
            case (src, tgt) => makeConverter(src, tgt)
          }
          data => for ((f, x) <- funcs.iterator zip data.clusterIterator) f(x)

        case (TArr(src, _), TArr(tgt, _)) =>
          val f = makeConverter(src, tgt)
          data => for (x <- data.flatIterator) f(x)

        case (TValue(Some(from)), TValue(Some(to))) =>
          if (from == to) {
            data => ()
          } else {
            val func = Units.convert(from, to)
            data => data.setValue(func(data.getValue))
          }

        case (TComplex(Some(from)), TComplex(Some(to))) =>
          val func = Units.convert(from, to)
          data => data.setComplex(func(data.getReal), func(data.getImag))

        case _ =>
          data => ()
      }
    }
  }

  /**
   * Calculate the index into an N-dimensional array with the given shape,
   * when doing a flat traversal in row-major order to the indices given
   * by idx. We use only the first N elements of the index array,
   * since this may include more indices to travers further into a data
   * object.
   */
  def flatIndex(shape: Array[Int], idx: Seq[Int]): Int = {
    val depth = shape.size
    var arrIdx = 0
    var stride = 1
    for (dim <- (depth - 1) to 0 by -1) {
      arrIdx += idx(dim) * stride
      stride *= shape(dim)
    }
    arrIdx
  }
}



// helpers for building and pattern-matching labrad data

object Cluster {
  def apply(elems: Data*) = {
    val data = Data(TCluster(elems.map(_.t): _*))
    val it = data.clusterIterator
    for (elem <- elems) {
      it.next().set(elem)
    }
    data
  }
  def unapplySeq(data: Data): Option[Seq[Data]] = data.t match {
    case TCluster(_*) => Some(data.clusterIterator.toSeq)
    case _ => None
  }
}

object Arr {
  private def make[T: Setter](a: Array[T], elemType: Type): Data = {
    val data = Data(TArr(elemType, 1))
    val m = a.length
    data.setArrayShape(m)
    val it = data.flatIterator
    for (i <- 0 until m) {
      it.next().set(a(i))
    }
    data
  }

  def apply(elems: Array[Data]): Data = {
    val elemType = if (elems.size == 0) TNone else elems(0).t
    make[Data](elems, elemType)
  }
  def apply(elems: Seq[Data]): Data = apply(elems.toArray)
  def apply(elem: Data, elems: Data*): Data = apply(elem +: elems)

  def apply(a: Array[Boolean]): Data = {
    val elem = TBool
    val data = Data(TArr(elem, 1))
    val m = a.length
    data.setArrayShape(m)
    val bytes = data.arrayBytes
    var i = 0
    while (i < m) {
      bytes.setBool(elem.dataWidth * i, a(i))
      i += 1
    }
    data
  }
  def apply(a: Array[Int]): Data = {
    val elem = TInt
    val data = Data(TArr(elem, 1))
    val m = a.length
    data.setArrayShape(m)
    val bytes = data.arrayBytes
    var i = 0
    while (i < m) {
      bytes.setInt(elem.dataWidth * i, a(i))
      i += 1
    }
    data
  }
  def apply(a: Array[Long]): Data = {
    val elem = TUInt
    val data = Data(TArr(elem, 1))
    val m = a.length
    data.setArrayShape(m)
    val bytes = data.arrayBytes
    var i = 0
    while (i < m) {
      bytes.setUInt(elem.dataWidth * i, a(i))
      i += 1
    }
    data
  }
  def apply(a: Array[String]): Data = make[String](a, TStr)
  def apply(a: Array[Double]): Data = {
    val elem = TValue()
    val data = Data(TArr(elem, 1))
    val m = a.length
    data.setArrayShape(m)
    val bytes = data.arrayBytes
    var i = 0
    while (i < m) {
      bytes.setDouble(elem.dataWidth * i, a(i))
      i += 1
    }
    data
  }
  def apply(a: Array[Double], units: String) = {
    val elem = TValue(units)
    val data = Data(TArr(elem, 1))
    val m = a.length
    data.setArrayShape(m)
    val bytes = data.arrayBytes
    var i = 0
    while (i < m) {
      bytes.setDouble(elem.dataWidth * i, a(i))
      i += 1
    }
    data
  }

  def unapplySeq(data: Data): Option[Seq[Data]] =
    if (data.isArray) Some(data.get[Array[Data]].toIndexedSeq)
    else None
}

object Arr2 {
  private def make[T: Setter](a: Array[Array[T]], elemType: Type) = {
    val data = Data(TArr(elemType, 2))
    val (m, n) = (a.length, if (a.length > 0) a(0).length else 0)
    data.setArrayShape(m, n)
    val it = data.flatIterator
    for (i <- 0 until m) {
      assert(a(i).length == a(0).length, "array must be rectangular")
      for (j <- 0 until n)
        it.next().set(a(i)(j))
    }
    data
  }

  def apply(a: Array[Array[Boolean]]) = make[Boolean](a, TBool)
  def apply(a: Array[Array[Int]]) = make[Int](a, TInt)
  def apply(a: Array[Array[Long]]) = make[Long](a, TUInt)
  def apply(a: Array[Array[Double]]) = make[Double](a, TValue())
  def apply(a: Array[Array[Double]], units: String) = make[Double](a, TValue(units))
  def apply(a: Array[Array[String]]) = make[String](a, TStr)
}

object Arr3 {
  private def make[T: Setter](a: Array[Array[Array[T]]], elemType: Type) = {
    val data = Data(TArr(elemType, 3))
    val (m, n, p) = (a.length,
                     if (a.length > 0) a(0).length else 0,
                     if (a.length > 0 && a(0).length > 0) a(0)(0).length else 0)
    data.setArrayShape(m, n, p)
    val it = data.flatIterator
    for (i <- 0 until m) {
      assert(a(i).length == a(0).length, "array must be rectangular")
      for (j <- 0 until n) {
        assert(a(i)(j).length == a(0)(0).length, "array must be rectangular")
        for (k <- 0 until p)
          it.next().set(a(i)(j)(k))
      }
    }
    data
  }

  def apply(a: Array[Array[Array[Boolean]]]) = make[Boolean](a, TBool)
  def apply(a: Array[Array[Array[Int]]]) = make[Int](a, TInt)
  def apply(a: Array[Array[Array[Long]]]) = make[Long](a, TUInt)
  def apply(a: Array[Array[Array[Double]]]) = make[Double](a, TValue())
  def apply(a: Array[Array[Array[Double]]], units: String) = make[Double](a, TValue(units))
  def apply(a: Array[Array[Array[String]]]) = make[String](a, TStr)
}

object NDArray {
  def unapply(data: Data): Option[Int] = data.t match {
    case TArr(_, depth) => Some(depth)
    case _ => None
  }
}

object DNone {
  def apply() = Data.NONE
  def unapply(data: Data): Boolean = data.isNone
}

object Bool {
  def apply(b: Boolean): Data = { val d = Data(TBool); d.setBool(b); d }
  def unapply(data: Data): Option[Boolean] =
    if (data.isBool) Some(data.getBool)
    else None
}

object UInt {
  def apply(l: Long): Data = { val d = Data(TUInt); d.setUInt(l); d }
  def unapply(data: Data): Option[Long] =
    if (data.isUInt) Some(data.getUInt)
    else None
}

object Integer {
  def apply(i: Int): Data = { val d = Data(TInt); d.setInt(i); d }
  def unapply(data: Data): Option[Int] =
    if (data.isInt) Some(data.getInt)
    else None
}

object Str {
  def apply(s: String): Data = { val d = Data(TStr); d.setString(s); d }
  def unapply(data: Data): Option[String] =
    if (data.isString) Some(data.getString)
    else None
}

object Bytes {
  def apply(s: Array[Byte]): Data = { val d = Data(TBytes); d.setBytes(s); d }
  def unapply(data: Data): Option[Array[Byte]] =
    if (data.isBytes || data.isString) Some(data.getBytes)
    else None
}

object Time {
  def apply(t: Date): Data = { val d = Data(TTime); d.setTime(t); d }
  def apply(t: DateTime): Data = { val d = Data(TTime); d.setTime(t); d }
  def apply(t: TimeStamp): Data = { val d = Data(TTime); d.setTime(t); d }
  def unapply(data: Data): Option[Date] =
    if (data.isTime) Some(data.getTime.toDate)
    else None
}

object Dbl {
  def apply(x: Double): Data = { val d = Data(TValue()); d.setValue(x); d }
  def unapply(data: Data): Option[Double] =
    if (data.isValue) Some(data.getValue)
    else None
}

object Value {
  def apply(v: Double): Data = { val d = Data(TValue()); d.setValue(v); d }
  def apply(v: Double, u: String): Data = { val d = Data(TValue(u)); d.setValue(v); d }
  def apply(v: Double, u: Option[String]): Data = u match {
    case None => apply(v)
    case Some(u) => apply(v, u)
  }
  def unapply(data: Data): Option[(Double, String)] = data.t match {
    case TValue(units) => Some((data.getValue, units.getOrElse(null)))
    case _ => None
  }
}

object Cplx {
  def apply(re: Double, im: Double): Data = { val d = Data(TComplex()); d.setComplex(re, im); d }
  def apply(re: Double, im: Double, u: String): Data = { val d = Data(TComplex(u)); d.setComplex(re, im); d }
  def apply(re: Double, im: Double, u: Option[String]): Data = u match {
    case None => apply(re, im)
    case Some(u) => apply(re, im, u)
  }
  def unapply(data: Data): Option[(Double, Double, String)] = data.t match {
    case TComplex(units) => Some((data.getReal, data.getImag, units.getOrElse(null)))
    case _ => None
  }
}

object Error {
  def apply(code: Int, msg: String, payload: Data = Data.NONE): Data = {
    val d = Data(TError(payload.t))
    d.setError(code, msg, payload)
    d
  }
  def apply(ex: Throwable): Data = ex match {
    case ex: LabradException => ex.toData
    case ex => apply(0, ex.toString)
  }
  def unapply(data: Data): Option[(Int, String, Data)] =
    if (data.isError) Some((data.getErrorCode, data.getErrorMessage, data.getErrorPayload))
    else None
}


// parsing data from string representation

object Parsers {

  import fastparse._
  import fastparse.MultiLineWhitespace._
  import org.labrad.util.Parsing._

  def parseData(s: String): Data = parseOrThrow(dataAll(_), s)

  def dataAll[T: P]: P[Data] = P( Whitespace ~ data ~ Whitespace ~ End )

  def data[T: P]: P[Data] = P( nonArrayData | array )

  def nonArrayData[T: P]: P[Data] =
    P( none | bool | complex | value | time | int | uint | bytes | string | cluster | map)

  def none[T: P]: P[Data] = P( "_" ).map { _ => Data.NONE }

  def bool[T: P]: P[Data] =
    P( Token("true").map { _ => Bool(true) }
     | Token("false").map { _ => Bool(false) }
     )

  def int[T: P]: P[Data] =
    P( Re("""[+-]\d+""").! ).map { s => Integer(s.substring(if (s.startsWith("+")) 1 else 0).toInt) } // i8, i16, i64

  def uint[T: P]: P[Data] =
    P( Re("""\d+""").! ).map { s => UInt(s.toLong) } // w8, w16, w64

  def bytes[T: P]: P[Data] =
    P( Re("b\"" + """([^"\p{Cntrl}\\]|\\[\\/bfnrtv"]|\\x[a-fA-F0-9]{2})*""" + "\"").!.map { s => Bytes(Translate.parseBytesLiteral(s, quote = '"')) }
     | Re("b'" + """([^'\p{Cntrl}\\]|\\[\\/bfnrtv']|\\x[a-fA-F0-9]{2})*""" + "'").!.map { s => Bytes(Translate.parseBytesLiteral(s, quote = '\'')) }
     )

  def string[T: P]: P[Data] =
    P( Re("\"" + """([^"\p{Cntrl}\\]|\\[\\/bfnrtv"]|\\u[a-fA-F0-9]{4})*""" + "\"").!.map { s => Str(Translate.parseStringLiteral(s, quote = '"')) }
     | Re("'" + """([^'\p{Cntrl}\\]|\\[\\/bfnrtv']|\\u[a-fA-F0-9]{4})*""" + "'").!.map { s => Str(Translate.parseStringLiteral(s, quote = '\'')) }
     )

  def time[T: P]: P[Data] =
    P( Re("""\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d{3})?Z""").! ).map { s => Time(new DateTime(s).toDate) }

  def value[T: P]: P[Data] =
    P( signedReal ~ units.!.? ).map { case (num, unit) => Value(num, unit) }

  def complex[T: P]: P[Data] =
    P( complexNum ~ units.!.? ).map { case (re, im, unit) => Cplx(re, im, unit) }

  def signedReal[T: P]: P[Double] =
    P( "+" ~ unsignedReal.map { x => x }
     | "-" ~ unsignedReal.map { x => -x }
     | unsignedReal
     )

  def unsignedReal[T: P]: P[Double] =
    P( Token("NaN").map { _ => Double.NaN }
     | Token("Infinity").map { _ => Double.PositiveInfinity }
     | Re("""(\d*\.\d+|\d+(\.\d*)?)[eE][+-]?\d+""").!.map { _.toDouble }
     | Re("""\d*\.\d+""").!.map { _.toDouble }
     )

  def complexNum[T: P]: P[(Double, Double)] =
    P( signedReal ~ ("+" | "-").! ~ unsignedReal ~ "i" ).map {
         case (re, "+", im) => (re, im)
         case (re, "-", im) => (re, -im)
       }

  def units[T: P] = P( firstTerm ~ (divTerm | mulTerm).rep )
  def firstTerm[T: P] = P( "1".? ~ divTerm | term )
  def mulTerm[T: P] = P( "*" ~ term )
  def divTerm[T: P] = P( "/" ~ term )
  def term[T: P] = P( termName ~ exponent.? )
  def termName[T: P] = Re("""[A-Za-z'"][A-Za-z'"0-9]*""")
  def exponent[T: P] = P( "^" ~ "-".? ~ number ~ ("/" ~ number).? )
  def number[T: P] = Re("""\d+""")

  def array[T: P]: P[Data] = P( arrND ).map { case (elems, typ, shape) =>
    val data = TreeData(TArr(typ, shape.size))
    data.setArrayShape(shape: _*)
    for ((data, elem) <- data.flatIterator zip elems.iterator) {
      data.set(elem)
    }
    data
  }

  def arrND[T: P]: P[(Array[Data], Type, List[Int])] =
    P( ("[" ~ nonArrayData.rep(sep = ",") ~ "]").map { elems =>
         val typ = if (elems.isEmpty) {
           TNone
         } else {
           val typ = elems(0).t
           require(elems forall (_.t == typ), s"all elements must be of type '$typ'")
           typ
         }
         (elems.toArray, typ, List(elems.size))
       }
     | ("[" ~ arrND.rep(sep = ",") ~ "]").map { subArrays =>
         // make sure all subarrays have the same shape and element type
         val (typ, shape) = (subArrays(0)._2, subArrays(0)._3)
         require(subArrays forall (_._2 == typ))
         require(subArrays forall (_._3 == shape))
         (subArrays.flatMap(_._1).toArray, typ, subArrays.size :: shape)
       }
     )

  def cluster[T: P]: P[Data] = P( "(" ~ data.rep(sep = ",") ~ ")" ).map { elems => Cluster(elems: _*) }

  def map[T: P]: P[Data] = P( "{" ~ mapItem.rep(sep = ",") ~ "}" ).map { items =>
    val keyTypes = items.map { case (key, value) => key.t }.toSet
    require(keyTypes.size <= 1, s"all map keys must have the same type: ${keyTypes.mkString(",")}")
    Cluster(items.map { case (key, value) => Cluster(key, value) }: _*)
  }

  def mapItem[T: P]: P[(Data, Data)] = P( data ~ ":" ~ data )
}

object Translate {

  private val translation = {
    // by default, use a two-digit hex escape code
    val lut = Array.tabulate[String](256) { b => """\x%02x""".format(b) }

    def translate(from: Byte, to: String): Unit = { lut((from + 256) % 256) = to }

    // printable ascii characters are not translated
    val ASCII_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 !@#$%^&*()-_=+|[]{}:;,.<>?/'"
    for (c <- ASCII_CHARS) translate(c.toByte, c.toString)

    // escape characters are preceded by a backslash
    translate('"',  """\"""")
    translate('\\', """\\""")
    translate('\b', """\b""")
    translate('\f', """\f""")
    translate('\n', """\n""")
    translate('\r', """\r""")
    translate('\t', """\t""")

    lut
  }

  def toBytesLiteral(bytes: Array[Byte], quote: Char = '"'): String = {
    val buf = new StringBuilder
    buf += 'b'
    buf += quote
    for (b <- bytes) {
      b.toChar match {
        case `quote` => buf ++= s"\\$quote"
        case '\\' => buf ++= """\\"""
        case '\n' => buf ++= """\n"""
        case '\r' => buf ++= """\r"""
        case '\t' => buf ++= """\t"""
        case c if isASCIIPrintable(c) => buf += c
        case c => buf ++= "\\x%02X".format(b) // byte escape
      }
    }
    buf += quote
    buf.toString
  }

  def toStringLiteral(str: String, quote: Char = '"'): String = {
    val buf = new StringBuilder
    buf += quote
    for (c <- str) {
      c match {
        case `quote` => buf ++= s"\\$quote"
        case '\\' => buf ++= """\\"""
        case '\n' => buf ++= """\n"""
        case '\r' => buf ++= """\r"""
        case '\t' => buf ++= """\t"""
        case c if isUnicodePrintable(c) => buf += c
        case c => buf ++= "\\u%04X".format(c.toInt) // generic unicode escape
      }
    }
    buf += quote
    buf.toString
  }

  def isASCIIPrintable(c: Char): Boolean = {
    c >= 32 && c < 127
  }

  def isUnicodePrintable(c: Char): Boolean = {
    c >= 32 && c < 127  // TODO: include more printable unicode chars from BMP
  }

  def parseBytesLiteral(s: String, quote: Char = '"'): Array[Byte] = {
    require(s(0) == 'b')
    require(s(1) == quote)
    require(s.last == quote)
    val trimmed = s.substring(2, s.length-1)
    var pos = 0
    val buf = Array.newBuilder[Byte]
    while (pos < trimmed.length) {
      buf += (trimmed(pos) match {
        case '\\' =>
          pos += 1
          trimmed(pos) match {
            case 'x' =>
              pos += 1
              val str = trimmed.substring(pos, pos+2)
              val byte = java.lang.Integer.parseInt(str, 16).toByte
              pos += 1
              byte
            case 'b' => 8
            case 't' => 9
            case 'n' => 10
            case 'v' => 11 // NOTE: this escape is not in standard java
            case 'f' => 12
            case 'r' => 13
            case '\\' => 92
            case c => c.toByte
          }
        case c =>
          c.toByte
      })
      pos += 1
    }
    buf.result()
  }

  def parseStringLiteral(s: String, quote: Char = '"'): String = {
    require(s.head == quote)
    require(s.last == quote)
    val trimmed = s.substring(1, s.length-1)
    var pos = 0
    val buf = new StringBuilder
    while (pos < trimmed.length) {
      buf += (trimmed(pos) match {
        case '\\' =>
          pos += 1
          trimmed(pos) match {
            case 'u' =>
              pos += 1
              val str = trimmed.substring(pos, pos+4)
              val char = java.lang.Integer.parseInt(str, 16).toChar
              pos += 3
              char
            case 'b' => 8
            case 't' => 9
            case 'n' => 10
            case 'v' => 11 // NOTE: this escape is not in standard java
            case 'f' => 12
            case 'r' => 13
            case '\\' => 92
            case c => c.toChar
          }
        case c =>
          c.toChar
      })
      pos += 1
    }
    buf.toString
  }

}


