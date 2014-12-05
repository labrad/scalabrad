package org.labrad
package data

import scala.language.implicitConversions

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, Serializable}
import java.nio.ByteOrder
import java.nio.ByteOrder.{BIG_ENDIAN, LITTLE_ENDIAN}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Date
import org.joda.time.{DateTime, DateTimeZone}
import org.labrad.errors.{LabradException, NonIndexableTypeException}
import org.labrad.types._
import scala.annotation.tailrec
import scala.collection.mutable.Buffer
import scala.math
import scala.reflect.ClassTag
import scala.util.parsing.combinator.RegexParsers

class ShapeTraversable(shape: Int*) {
  def foreachShape(func: Array[Int] => Unit) {
    val nDims = shape.size
    val idx = Array.ofDim[Int](nDims)
    def traverse(k: Int) {
      if (k == nDims)
        func(idx)
      else
        for (i <- 0 until shape(nDims)) {
          idx(k) = i
          traverse(k + 1)
        }
    }
    traverse(0)
  }
}

object ShapeTraversers {
  implicit def arrayToShapeTraversable(shape: Array[Int]) = new ShapeTraversable(shape: _*)
  implicit def seqToShapeTraversable(shape: Seq[Int]) = new ShapeTraversable(shape: _*)
}

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

trait IBool {
  def get: Boolean
  def set(b: Boolean)
}

trait IInt {
  def get: Int
}

trait IUInt {
  def get: Long
}

trait IValue {
  def get: Double
  def units: String
}

trait IComplex {
  def real: Double
  def imag: Double
  def units: String
}

trait IBytes {
  def get: Array[Byte]
}

trait IStr {
  def get: String
}

trait IArr { // this: Data =>
  def apply(idx: Int*): IData
  def size: Int
  def nDims: Int
  def shape: Array[Int]

  /*
  def toSeq[T: ClassManifest](getter: IData => T): Seq[T] = t match {
    case TArr(_, 1) => Seq.tabulate(size)(i => getter(this(i)))
    case _ => sys.error("Must be an array")
  }

  def toBytes(implicit bo: ByteOrder): Array[Byte] = {
    val os = EndianAwareOutputStream(new ByteArrayOutputStream)
    for (dim <- shape)
      os.writeUInt(dim)
    val idx = Array.ofDim[Int](nDims)
    def traverse(k: Int) {
      if (k == nDims)
        os.write(this(idx: _*).toBytes)
      else
        for (i <- 0 until shape(k)) {
          idx(k) = i
          traverse(k + 1)
        }
    }
    traverse(0)
    os.toByteArray
  }
  */
}

trait ICluster { // this: Data =>
  def apply(idx: Int): IData
  def size: Int

  /*
  override def toBytes(implicit byteOrder: ByteOrder): Array[Byte] =
    (0 until size).flatMap(this(_).toBytes).toArray
  */
}

trait IError {
  def code: Int
  def message: String
  def payload: IData
}

trait IData { this: Data =>
  def t: Type

  def ~==(other: Any): Boolean

  def toBytes(implicit byteOrder: ByteOrder): Array[Byte]

  def apply(idx: Int*): Data

  def set(other: Data)
  def update(i: Int, other: Data) { this(i).set(other) }
  def update(i: Int, j: Int, other: Data) { this(i, j).set(other) }
  def update(i: Int, j: Int, k: Int, other: Data) { this(i, j, k).set(other) }
  def update(i: Int, j: Int, k: Int, l: Int, other: Data) { this(i, j, k, l).set(other) }

  // type checks
  def isNone = t == TNone
  def isBool = t == TBool
  def isInt = t == TInt
  def isUInt = t == TUInt
  def isBytes = t == TStr
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

  def asBool: IBool
  def asInt: IInt
  def asUInt: IUInt
  def asValue: IValue
  def asComplex: IComplex
  def asBytes: IBytes
  def asStr: IStr
  def asArray: IArr
  def asCluster: ICluster
  def asError: IError

  def arraySize: Int
  def arrayShape: Array[Int]
  def setArrayShape(shape: Int*): Data

  def arraySize_=(size: Int) = setArrayShape(size)
  def arrayShape_=(size: Int) = setArrayShape(size)
  def arrayShape_=(shape: (Int, Int)) = setArrayShape(shape._1, shape._2)
  def arrayShape_=(shape: (Int, Int, Int)) = setArrayShape(shape._1, shape._2, shape._3)
  def arrayShape_=(shape: (Int, Int, Int, Int)) = setArrayShape(shape._1, shape._2, shape._3, shape._4)
  def arrayShape_=(shape: Array[Int]) = setArrayShape(shape: _*)
  def setArraySize(size: Int, idx: Int*): IData = { this(idx: _*).arraySize = size; this }

  def clusterSize: Int = t match {
    case TCluster(elems @ _*) => elems.size
    case _ => sys.error("clusterSize is only defined for clusters")
  }

  // getters
  def getBool: Boolean
  def getInt: Int
  def getUInt: Long
  def getBytes: Array[Byte]
  def getString: String = new String(getBytes, UTF_8)
  def getValue: Double
  def getComplex: Complex
  def getReal: Double
  def getImag: Double
  def getTime: TimeStamp
  def getError: IError

  // setters
  def setBool(data: Boolean): Data
  def setInt(data: Int): Data
  def setUInt(data: Long): Data
  def setBytes(data: Array[Byte]): Data
  def setString(data: String): Data
  def setValue(data: Double): Data
  def setComplex(data: Complex): Data
  def setComplex(re: Double, im: Double): Data
  def setTime(timestamp: TimeStamp): Data
  def setError(code: Int, message: String, payload: Data): Data

  def setTime(date: Date): Data = setTime(TimeStamp(date))
  def setTime(dateTime: DateTime): Data = setTime(TimeStamp(dateTime))

  // array getters
  def getArray[T: ClassTag](func: Data => T): Array[T] = t match {
    case TArr(_, 1) => Array.tabulate(arraySize)(i => func(this(i)))
    case _ => sys.error("must be a 1D array")
  }
  def getDataArray = getArray(data => data)
  def getBoolArray = getArray(_.getBool)
  def getIntArray = getArray(_.getInt)
  def getUIntArray = getArray(_.getUInt)
  def getStringArray = getArray(_.getString)
  def getValueArray = getArray(_.getValue)

  // seq getters
  def getSeq[T: ClassTag](func: Data => T): Seq[T] = t match {
    case TArr(_, 1) => Seq.tabulate(arraySize)(i => func(this(i)))
    case _ => sys.error("must be a 1D array")
  }
  def getDataSeq = getSeq(data => data)
  def getBoolSeq = getSeq(_.getBool)
  def getIntSeq = getSeq(_.getInt)
  def getUIntSeq = getSeq(_.getUInt)
  def getStringSeq = getSeq(_.getString)
  def getDoubleSeq = getSeq(_.getValue)
  def getComplexSeq = getSeq(_.getComplex)

  // seq setters
  def setSeq[T](data: Seq[T])(implicit setter: Setter[T]): Data = t match {
    case TArr(elem, 1) =>
      if (data.size > 0 && !setter.t.accepts(elem))
        sys.error("wrong element type")
      else {
        arraySize = data.size
        for (i <- 0 until data.size)
          setter.set(this(i), data(i))
        this
      }
    case _ =>
      sys.error("must be a 1D array")
  }

  def setBoolSeq(data: Seq[Boolean]) = setSeq(data)(Setters.boolSetter)
  def setIntSeq(data: Seq[Int]) = setSeq(data)(Setters.intSetter)
  def setUIntSeq(data: Seq[Long]) = setSeq(data)(Setters.uintSetter)
  def setStringSeq(data: Seq[String]) = setSeq(data)(Setters.stringSetter)
  def setDateSeq(data: Seq[Date]) = setSeq(data)(Setters.dateSetter)
  def setDoubleSeq(data: Seq[Double]) = setSeq(data)(Setters.valueSetter)
  def setComplexSeq(data: Seq[Complex]) = setSeq(data)(Setters.complexSetter)

  // indexed setters
  def setBool(b: Boolean, idx: Int*): Data = { this(idx: _*).setBool(b); this }
  def setInt(i: Int, idx: Int*): Data = { this(idx: _*).setInt(i); this }
  def setUInt(u: Long, idx: Int*): Data = { this(idx: _*).setUInt(u); this }
  def setBytes(data: Array[Byte], idx: Int*): Data = { this(idx: _*).setBytes(data); this }
  def setString(data: String, idx: Int*): Data = { this(idx: _*).setString(data); this }
  def setValue(data: Double, idx: Int*): Data = { this(idx: _*).setValue(data); this }
  def setComplex(data: Complex, idx: Int*): Data = { this(idx: _*).setComplex(data); this }
  def setComplex(re: Double, im: Double, idx: Int*): Data = { this(idx: _*).setComplex(re, im); this }
  def setTime(date: Date, idx: Int*): Data = { this(idx: _*).setTime(date); this }

  // indexed seq setters
  def setBoolSeq(data: Seq[Boolean], idx: Int*): Data = { this(idx: _*).setBoolSeq(data); this }
  def setIntSeq(data: Seq[Int], idx: Int*): Data = { this(idx: _*).setIntSeq(data); this }
  def setUIntSeq(data: Seq[Long], idx: Int*): Data = { this(idx: _*).setUIntSeq(data); this }
  def setStringSeq(data: Seq[String], idx: Int*): Data = { this(idx: _*).setStringSeq(data); this }
  def setDateSeq(data: Seq[Date], idx: Int*): Data = { this(idx: _*).setDateSeq(data); this }
  def setDoubleSeq(data: Seq[Double], idx: Int*): Data = { this(idx: _*).setDoubleSeq(data); this }
  def setComplexSeq(data: Seq[Complex], idx: Int*): Data = { this(idx: _*).setComplexSeq(data); this }
}

/**
 * The Data class encapsulates the data format used to communicate between
 * LabRAD servers and clients.  This data format is based on the
 * capabilities of LabVIEW, from National Instruments.  Each piece of LabRAD
 * data has a Type object which is specified by a String type tag.
 */
class Data protected(val t: Type, buf: Array[Byte], ofs: Int, heap: Buffer[Array[Byte]])(implicit byteOrder: ByteOrder)
extends IData with Serializable with Cloneable {

  import RichByteArray._ // implicitly add endian-aware get* and set* methods to Array[Byte]
  import ShapeTraversers._ // implicitly allow traversing over a shape array

  override def clone = Data.copy(this, Data(this.t))

  override def equals(other: Any): Boolean = other match {
    case other: Data => Data.isEqual(this, other)
    case _ => false
  }

  def ~==(other: Any): Boolean = other match {
    case other: Data => Data.approxEqual(this, other)
    case _ => false
  }

  private def cast(newType: Type) = new Data(newType, buf, ofs, heap)

  def set(other: Data) { Data.copy(other, this) }

  def toBytes(implicit outputOrder: ByteOrder): Array[Byte] = {
    val bs = new ByteArrayOutputStream
    val os = EndianAwareOutputStream(bs)(outputOrder)
    flatten(os, t, buf, ofs)
    bs.toByteArray
  }

  private def flatten(os: EndianAwareOutputStream, t: Type, buf: Array[Byte], ofs: Int) {
    if (t.fixedWidth && os.byteOrder == byteOrder)
      os.write(buf, ofs, t.dataWidth)
    else {
      t match {
        case TNone =>

        case TBool =>
          os.writeBool(buf.getBool(ofs))

        case TInt =>
          os.writeInt(buf.getInt(ofs))

        case TUInt =>
          os.writeUInt(buf.getUInt(ofs))

        case TValue(_) =>
          os.writeDouble(buf.getDouble(ofs))

        case TComplex(_) =>
          os.writeDouble(buf.getDouble(ofs))
          os.writeDouble(buf.getDouble(ofs + 8))

        case TTime =>
          os.writeLong(buf.getLong(ofs))
          os.writeLong(buf.getLong(ofs + 8))

        case TStr =>
          val strBuf = heap(buf.getInt(ofs))
          os.writeInt(strBuf.length)
          os.write(strBuf)

        case TArr(elem, depth) =>
          // write arr shape and compute total number of elements in the list
          var size = 1
          for (i <- 0 until depth) {
            val dim = buf.getInt(ofs + 4*i)
            os.writeInt(dim)
            size *= dim
          }

          // write arr data
          val arrBuf = heap(buf.getInt(ofs + 4*depth))
          if (elem.fixedWidth && os.byteOrder == byteOrder) {
            // for fixed-width data, just copy in one big chunk
            os.write(arrBuf, 0, elem.dataWidth * size)
          } else {
            // for variable-width data, flatten recursively
            for (i <- 0 until size)
              flatten(os, elem, arrBuf, elem.dataWidth * i)
          }

        case t: TCluster =>
          for ((elem, delta) <- t.elems zip t.offsets)
            flatten(os, elem, buf, ofs + delta)

        case TError(payload) =>
          flatten(os, TCluster(TInt, TStr, payload), buf, ofs)
      }
    }
  }

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

    case TStr => Translate.bytesToString(getBytes)

    case TArr(elem, depth) =>
      val shape = arrayShape
      val idx = Array.ofDim[Int](depth)
      val sb = new StringBuilder
      def buildString(k: Int) {
        sb += '['
        for (i <- 0 until shape(k)) {
          idx(k) = i
          if (i > 0) sb += ','
          if (k == shape.length - 1)
            sb ++= this(idx: _*).toString
          else
            buildString(k + 1)
        }
        sb += ']'
      }
      buildString(0)
      sb.toString

    case TCluster(_*) =>
      "(" + (0 until clusterSize).map(this(_)).mkString(",") + ")"

    case TError(_) =>
      "Error(" + getErrorCode + ", " + getErrorMessage + ", " + getErrorPayload + ")"
  }

  def convertTo(pattern: String): Data = convertTo(Pattern(pattern))
  def convertTo(pattern: Pattern): Data = {
    // XXX: converting empty lists is a special case.
    // If we are converting to a list with the same number of dimensions and
    // the target element type is a concrete type, we allow this conversion.
    // We test whether the pattern is a concrete type by parsing it as a type.
    val empty = (t, pattern) match {
      case (TArr(TNone, depth), PArr(pat, pDepth)) if depth == pDepth && arrayShape.product == 0 =>
        val elemType = try {
          Some(Type(pat.toString))
        } catch {
          case ex: Throwable => None
        }
        elemType.map(t => cast(TArr(t, depth)))
      case _ =>
        None
    }

    empty.getOrElse {
      pattern(t) match {
        case Some(tgt) =>
          makeConverter(t, tgt) map { f => f(this) }
          cast(tgt)
        case None =>
          sys.error(s"cannot convert data from '$t' to '$pattern'")
      }
    }
  }

  private def makeConverter(src: Type, tgt: Type): Option[Data => Unit] =
    if (src == tgt)
      None
    else
      (src, tgt) match {
        case (TCluster(srcs @ _*), TCluster(tgts @ _*)) =>
          val funcs = (srcs zip tgts).zipWithIndex flatMap {
            case ((src, tgt), i) => makeConverter(src, tgt) map (f => (f, i))
          }
          if (funcs.isEmpty)
            None
          else
            Some(data => for ((f, i) <- funcs) f(data(i)))

        case (TArr(src, _), TArr(tgt, _)) =>
          makeConverter(src, tgt) map { func =>
            data => data flatIterate { (elem, i) => func(elem) }
          }

        case (TValue(Some(from)), TValue(Some(to))) =>
          if (from == to)
            None
          else {
            val func = Units.convert(from, to)
            Some(data => data.setValue(func(data.getValue)))
          }

        case (TComplex(Some(from)), TComplex(Some(to))) =>
          val func = Units.convert(from, to)
          Some(data => data.setComplex(func(data.getReal), func(data.getImag)))

        case _ =>
          None
      }

  /**
   * Get a Data subobject at the specified array of indices.  Note that
   * this returns a view rather than a copy, so any modifications to
   * the subobject will be reflected in the original data.
   */
  def apply(idx: Int*) = {
    @tailrec
    def find(t: Type, buf: Array[Byte], ofs: Int, idx: Seq[Int]): (Type, Array[Byte], Int) =
      if (idx.isEmpty)
        (t, buf, ofs)
      else
        t match {
          case TArr(elem, depth) =>
            if (idx.size < depth)
              sys.error("not enough indices for array")
            val arrBuf = heap(buf.getInt(ofs + 4 * depth))
            val arrOfs = {
              var temp = 0
              var stride = 1
              val shape = Seq.tabulate(depth)(i => buf.getInt(ofs + 4 * i))
              for (dim <- (depth - 1) to 0 by -1) {
                temp += elem.dataWidth * idx(dim) * stride
                stride *= shape(dim)
              }
              temp
            }
            find(elem, arrBuf, arrOfs, idx.drop(depth))

          case t: TCluster =>
            find(t.elems(idx.head), buf, ofs + t.offsets(idx.head), idx.tail)

          case _ =>
            sys.error("type " + t + " is not indexable")
        }
    val (t, buf, ofs) = find(this.t, this.buf, this.ofs, idx)
    new Data(t, buf, ofs, heap)
  }

  /** Apply the given function to every element in an ND array */
  def flatIterate(func: (Data, Int) => Unit): Unit = t match {
    case TArr(elem, depth) =>
      val size = arrayShape.product
      val arrBuf = heap(buf.getInt(ofs + 4 * depth))
      for (i <- 0 until size)
        func(new Data(elem, arrBuf, i * elem.dataWidth, heap), i)
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

  def setArrayShape(shape: Int*) = t match {
    case TArr(elem, depth) =>
      require(shape.length == depth)
      val size = shape.product
      for (i <- 0 until depth)
        buf.setInt(ofs + 4*i, shape(i))
      val arrBuf = Data.newByteArray(elem.dataWidth * size)
      val heapIndex = buf.getInt(ofs + 4*depth)
      if (heapIndex == -1) {
        buf.setInt(ofs + 4*depth, heap.size)
        heap += arrBuf
      } else {
        val oldBuf = heap(heapIndex)
        Array.copy(oldBuf, 0, arrBuf, 0, math.min(oldBuf.size, arrBuf.size))
        heap(heapIndex) = arrBuf
      }
      this
    case _ =>
      sys.error("arrayShape can only be set for arrays")
  }

  //
  def asBool = t match {
    case TBool =>
      new IBool {
        def get = buf.getBool(ofs)
        def set(b: Boolean) {
          buf.setBool(ofs, b)
        }
      }
    case _ =>
      sys.error(s"cannot convert $t to b")
  }

  def asInt = t match {
    case TInt =>
      new IInt {
        def get = buf.getInt(ofs)
        def set(i: Int) {
          buf.setInt(ofs, i)
        }
      }
    case _ =>
      sys.error(s"cannot convert $t to i")
  }

  def asUInt = t match {
    case TUInt =>
      new IUInt {
        def get = buf.getUInt(ofs)
        def set(l: Long) {
          buf.setUInt(ofs, l)
        }
      }
    case _ =>
      sys.error(s"cannot convert $t to w")
  }

  def asValue = t match {
    case TValue(u) =>
      new IValue {
        def get = buf.getDouble(ofs)
        def units = u.getOrElse("")
        def set(d: Double) {
          buf.setDouble(ofs, d)
        }
      }
    case _ =>
      sys.error(s"cannot convert $t to v")
  }

  def asComplex = t match {
    case TComplex(u) =>
      new IComplex {
        def real = buf.getDouble(ofs)
        def imag = buf.getDouble(ofs + 8)
        def units = u.getOrElse("")
        def set(re: Double, im: Double) {
          buf.setDouble(ofs, re)
          buf.setDouble(ofs + 8, im)
        }
        def set(c: Complex) {
          set(c.real, c.imag) }
      }
    case _ =>
      sys.error(s"cannot convert $t to c")
  }

  def asBytes = t match {
    case TStr =>
      new IBytes {
        def get = heap(buf.getInt(ofs))
      }
    case _ =>
      sys.error(s"cannot convert $t to s")
  }

  def asStr = t match {
    case TStr =>
      new IStr {
        def get = new String(heap(buf.getInt(ofs)))
      }
    case _ =>
      sys.error(s"cannot convert $t to s")
  }

  def asArray = t match {
    case TArr(elem, d) =>
      new IArr {
        def size = shape.product
        def nDims = d
        def shape = Array.tabulate(d)(i => buf.getInt(ofs + 4 * i))
        def apply(idx: Int*): IData = {
          if (idx.size != d)
            sys.error("incorrect number of indices for array")
          val arrBuf = heap(buf.getInt(ofs + 4 * d))
          val arrOfs = {
            var temp = 0
            var stride = 1
            val shape = this.shape
            for (dim <- (d - 1) to 0 by -1) {
              temp += elem.dataWidth * idx(dim) * stride
              stride *= shape(dim)
            }
            temp
          }
          new Data(elem, arrBuf, arrOfs, heap)
        }
      }
    case _ =>
      sys.error(s"cannot convert $t to *")
  }

  def asCluster = t match {
    case tc @ TCluster(elems @ _*) =>
      new ICluster {
        def size = elems.size
        def apply(i: Int) =
          new Data(elems(i), buf, ofs + tc.offset(i), heap)
      }
    case _ =>
      sys.error(s"cannot convert $t to ()")
  }

  def asError = t match {
    case TError(p) =>
      new IError {
        def code = buf.getInt(ofs)
        def message = new String(heap(buf.getInt(ofs + 4)), UTF_8)
        def payload = new Data(p, buf, ofs + 8, heap)
      }
    case _ =>
      sys.error(s"cannot convert $t to E")
  }

  // getters
  def getBool = { require(isBool); buf.getBool(ofs) }
  def getInt = { require(isInt); buf.getInt(ofs) }
  def getUInt = { require(isUInt); buf.getUInt(ofs) }
  def getBytes = { require(isBytes); heap(buf.getInt(ofs)) }
  def getValue = { require(isValue); buf.getDouble(ofs) }
  def getReal = { require(isComplex); buf.getDouble(ofs) }
  def getImag = { require(isComplex); buf.getDouble(ofs + 8) }
  def getComplex = { require(isComplex); Complex(buf.getDouble(ofs), buf.getDouble(ofs + 8)) }
  def getTime = { require(isTime); TimeStamp(buf.getLong(ofs), buf.getLong(ofs + 8)) }

  def getError = new IError {
    def code = getErrorCode
    def message = getErrorMessage
    def payload = getErrorPayload
  }
  def getErrorCode = { require(isError); buf.getInt(ofs) }
  def getErrorMessage = { require(isError); new String(heap(buf.getInt(ofs + 4)), UTF_8) }
  def getErrorPayload = t match {
    case TError(payload) => new Data(payload, buf, ofs + 8, heap)
    case _ => sys.error("errorPayload is only defined for errors")
  }

  // setters
  def setBool(b: Boolean): Data = {
    require(isBool)
    buf.setBool(ofs, b)
    this
  }

  def setInt(i: Int): Data = {
    require(isInt)
    buf.setInt(ofs, i)
    this
  }

  def setUInt(u: Long): Data = {
    require(isUInt)
    buf.setUInt(ofs, u)
    this
  }

  def setBytes(bytes: Array[Byte]): Data = {
    require(isString)
    var heapIndex = buf.getInt(ofs)
    if (heapIndex == -1) {
      // not yet set in the heap
      buf.setInt(ofs, heap.size)
      heap += bytes
    } else
      // already set in the heap, reuse old spot
      heap(heapIndex) = bytes
    this
  }

  def setString(s: String): Data = setBytes(s.getBytes(UTF_8))

  def setValue(d: Double): Data = {
    require(isValue)
    buf.setDouble(ofs, d)
    this
  }

  def setComplex(c: Complex): Data = setComplex(c.real, c.imag)

  def setComplex(re: Double, im: Double): Data = {
    require(isComplex)
    buf.setDouble(ofs, re)
    buf.setDouble(ofs + 8, im)
    this
  }

  def setTime(timestamp: TimeStamp): Data = {
    require(isTime)
    buf.setLong(ofs, timestamp.seconds)
    buf.setLong(ofs + 8, timestamp.fraction)
    this
  }

  def setError(code: Int, message: String, payload: Data = Data.NONE) = t match {
    case TError(payloadType) =>
      val data = cast(TCluster(TInt, TStr, payloadType))
      data(0).setInt(code)
      data(1).setString(message)
      data(2).set(payload)
      this
    case _ => sys.error("Data type must be error")
  }
}

object Data {
  val NONE = Data("")

  import RichByteArray._
  import ShapeTraversers._

  def apply(tag: String): Data = apply(Type(tag))
  def apply(t: Type)(implicit bo: ByteOrder = BIG_ENDIAN): Data =
    new Data(t, newByteArray(t.dataWidth), 0, newHeap)

  def parse(data: String): Data = Parsers.parseData(data)

  def newByteArray(length: Int) = Array.fill[Byte](length)(0xFF.toByte)
  def newHeap = Buffer.empty[Array[Byte]]

  def copy(src: Data, dest: Data): Data = {
    src match {
      case DNone() => assert(dest.isNone)
      case Bool(b) => dest.setBool(b)
      case Integer(i) => dest.setInt(i)
      case UInt(w) => dest.setUInt(w)
      case Value(v, u) => dest.setValue(v) // TODO: units?
      case Cplx(re, im, u) => dest.setComplex(re, im) // TODO: units?
      case Time(t) => dest.setTime(t)
      case Bytes(s) => dest.setBytes(s)
      case NDArray(depth) =>
        val shape = src.arrayShape
        dest.setArrayShape(shape: _*)
        val indices = Array.ofDim[Int](shape.length)
        def copyArr(k: Int) {
          for (i <- 0 until shape(k)) {
            indices(k) = i
            if (k == shape.length - 1)
              copy(src(indices: _*), dest(indices: _*))
            else
              copyArr(k + 1)
          }
        }
        copyArr(0)

      case Cluster(_*) =>
        for (i <- 0 until src.clusterSize)
          copy(src(i), dest(i))

      case Error(code, msg, payload) =>
        dest.setError(code, msg, payload)

      case _ =>
        println(src)
        println(src.t)
        println(dest.t)
        sys.error("Not implemented!")
    }
    dest
  }


  // unflattening from bytes
  def fromBytes(buf: Array[Byte], t: Type)(implicit bo: ByteOrder): Data =
    fromBytes(new ByteArrayInputStream(buf), t)

  def fromBytes(is: InputStream, t: Type)(implicit bo: ByteOrder): Data = {
    val in = EndianAwareInputStream(is)
    val buf = Array.ofDim[Byte](t.dataWidth)
    val heap = newHeap
    def unflatten(t: Type, buf: Array[Byte], ofs: Int) {
      if (t.fixedWidth)
        in.read(buf, ofs, t.dataWidth)
      else
        t match {
          case TStr =>
            val len = in.readInt
            val strBuf = Array.ofDim[Byte](len)
            buf.setInt(ofs, heap.size)
            heap += strBuf
            in.read(strBuf, 0, len)

          case TArr(elem, depth) =>
            var size = 1
            for (i <- 0 until depth) {
              val dim = in.readInt
              buf.setInt(ofs + 4 * i, dim)
              size *= dim
            }
            val arrBuf = Array.ofDim[Byte](elem.dataWidth * size)
            buf.setInt(ofs + 4 * depth, heap.size)
            heap += arrBuf
            if (elem.fixedWidth)
              in.read(arrBuf, 0, elem.dataWidth * size)
            else
              for (i <- 0 until size)
                unflatten(elem, arrBuf, elem.dataWidth * i)

          case t: TCluster =>
            for ((elem, delta) <- t.elems zip t.offsets)
              unflatten(elem, buf, ofs + delta)

          case TError(payload) =>
            unflatten(TCluster(TInt, TStr, payload), buf, ofs)

          case _ =>
            sys.error("missing case to handle non-fixed-width type: " + t)
        }
    }
    unflatten(t, buf, 0)
    new Data(t, buf, 0, heap)
  }

  // equality testing
  def isEqual(src: Data, dst: Data): Boolean = src match {
    case DNone() => dst match {
      case DNone() => true
      case _ => false
    }
    case Bool(b) => dst match {
      case Bool(`b`) => true
      case _ => false
    }
    case Integer(i) => dst match {
      case Integer(`i`) => true
      case _ => false
    }
    case UInt(w) => dst match {
      case UInt(`w`) => true
      case _ => false
    }
    case Value(v, u) => dst match {
      case Value(`v`, `u`) => true
      case _ => false
    }
    case Cplx(re, im, u) => dst match {
      case Cplx(`re`, `im`, `u`) => true
      case _ => false
    }
    case Time(t) => dst match {
      case Time(`t`) => true
      case _ => false
    }
    case Str(s) => dst match {
      case Str(`s`) => true
      case _ => false
    }
    case NDArray(depth) => dst match {
      case NDArray(`depth`) =>
        val shape = src.arrayShape
        (shape.toSeq == dst.arrayShape.toSeq) && {
          val indices = Array.ofDim[Int](depth)
          def arrEqual(k: Int): Boolean =
            (0 until shape(k)) forall { i =>
              indices(k) = i
              if (k == shape.length - 1)
                src(indices: _*) == dst(indices: _*)
              else
                arrEqual(k + 1)
            }
          arrEqual(0)
        }
      case _ => false
    }
    case Cluster(elems @ _*) => dst match {
      case Cluster(others @ _*) => elems == others
      case _ => false
    }
    case Error(code, msg, payload) => dst match {
      case Error(`code`, `msg`, `payload`) => true
      case _ => false
    }
    case _ =>
      sys.error("isEqual missing case for type " + src.t)
  }


  def approxEqual(src: Data, dst: Data): Boolean = src match {
    case Value(v1, u) => dst match {
      case Value(v2, `u`) => approxEqual(v1, v2)
      case _ => false
    }
    case Cplx(r1, i1, u) => dst match {
      case Cplx(r2, i2, `u`) => approxEqual(r1, r2) && approxEqual(i1, i2)
      case _ => false
    }
    case Time(t1) => dst match {
      case Time(t2) => math.abs(t1.getTime - t2.getTime) <= 1
      case _ => false
    }
    case NDArray(depth) => dst match {
      case NDArray(`depth`) =>
        val shape = src.arrayShape
        shape.toSeq == dst.arrayShape.toSeq && {
          val indices = Array.ofDim[Int](shape.length)
          def arrApproxEqual(k: Int): Boolean =
            (0 until shape(k)) forall { i =>
              indices(k) = i
              if (k == shape.length - 1)
                src(indices: _*) ~== dst(indices: _*)
              else
                arrApproxEqual(k + 1)
            }
          arrApproxEqual(0)
        }
      case _ => false
    }
    case Cluster(elems @ _*) => dst match {
      case Cluster(others @ _*) if others.size == elems.size =>
        (elems zip others) forall { case (elem, other) => elem ~== other }
      case _ => false
    }
    case _ => src == dst
  }

  private def approxEqual(a: Double, b: Double, tol: Double = 1e-9): Boolean = {
    math.abs(a-b) < 5 * tol * math.abs(a+b) || math.abs(a-b) < Double.MinPositiveValue
  }
}



// helpers for building and pattern-matching labrad data

object Cluster {
  def apply(elems: Data*) = {
    val data = Data(TCluster(elems.map(_.t): _*))
    for ((elem, i) <- elems.zipWithIndex)
      data(i) = elem
    data
  }
  def unapplySeq(data: Data): Option[Seq[Data]] = data.t match {
    case TCluster(_*) => Some(Seq.tabulate(data.clusterSize)(data(_)))
    case _ => None
  }
}

object Arr {
  def apply(elems: Seq[Data]): Data = {
    val elemType = if (elems.size == 0) TNone else elems(0).t
    val data = Data(TArr(elemType))
    data.arraySize = elems.size
    for ((elem, i) <- elems.zipWithIndex)
      data(i) = elem
    data
  }
  def apply(elems: Array[Data]): Data = apply(elems.toSeq)
  def apply(elem: Data, elems: Data*): Data = apply(elem +: elems)

  def apply[T](elems: Seq[T])(implicit setter: Setter[T]): Data = {
    val data = Data(TArr(setter.t))
    data.setSeq(elems)
    data
  }
  def apply[T](elems: Array[T])(implicit setter: Setter[T]): Data = apply(elems.toSeq)
  def apply[T](elem: T, elems: T*)(implicit setter: Setter[T]): Data = apply(elem +: elems)

  def apply(a: Array[Boolean]): Data = apply[Boolean](a)(Setters.boolSetter)
  def apply(a: Array[Int]): Data = apply[Int](a)(Setters.intSetter)
  def apply(a: Array[Long]): Data = apply[Long](a)(Setters.uintSetter)
  def apply(a: Array[String]): Data = apply[String](a)(Setters.stringSetter)
  def apply(a: Array[Double]): Data = apply[Double](a)(Setters.valueSetter)
  def apply(a: Array[Double], units: String) = apply[Double](a)(Setters.valueSetter(units))

  def unapplySeq(data: Data): Option[Seq[Data]] =
    if (data.isArray) Some(data.getDataArray)
    else None
}

object Arr2 {
  private def make[T](a: Array[Array[T]], elemType: Type, setter: (Data, T, Int, Int) => Unit) = {
    val data = Data(TArr(elemType, 2))
    val (m, n) = (a.length, if (a.length > 0) a(0).length else 0)
    data.setArrayShape(m, n)
    for (i <- 0 until m) {
      assert(a(i).length == a(0).length, "array must be rectangular")
      for (j <- 0 until n)
       setter(data, a(i)(j), i, j)
    }
    data
  }

  def apply(a: Array[Array[Boolean]]) =
    make(a, TBool, (data, elem: Boolean, i, j) => data.setBool(elem, i, j))

  def apply(a: Array[Array[Int]]) =
    make(a, TInt, (data, elem: Int, i, j) => data.setInt(elem, i, j))

  def apply(a: Array[Array[Long]]) =
    make(a, TUInt, (data, elem: Long, i, j) => data.setUInt(elem, i, j))

  def apply(a: Array[Array[Double]]) =
    make(a, TValue(), (data, elem: Double, i, j) => data.setValue(elem, i, j))

  def apply(a: Array[Array[Double]], units: String) =
    make(a, TValue(units), (data, elem: Double, i, j) => data.setValue(elem, i, j))

  def apply(a: Array[Array[String]]) =
    make(a, TStr, (data, elem: String, i, j) => data.setString(elem, i, j))
}

object Arr3 {
  private def make[T](a: Array[Array[Array[T]]], elemType: Type, setter: (Data, T, Int, Int, Int) => Unit) = {
    val data = Data(TArr(elemType, 3))
    val (m, n, p) = (a.length,
                     if (a.length > 0) a(0).length else 0,
                     if (a.length > 0 && a(0).length > 0) a(0)(0).length else 0)
    data.setArrayShape(m, n, p)
    for (i <- 0 until m) {
      assert(a(i).length == a(0).length, "array must be rectangular")
      for (j <- 0 until n) {
        assert(a(i)(j).length == a(0)(0).length, "array must be rectangular")
        for (k <- 0 until p)
          setter(data, a(i)(j)(k), i, j, k)
      }
    }
    data
  }

  def apply(a: Array[Array[Array[Boolean]]]) =
    make(a, TBool, (data, elem: Boolean, i, j, k) => data.setBool(elem, i, j, k))

  def apply(a: Array[Array[Array[Int]]]) =
    make(a, TInt, (data, elem: Int, i, j, k) => data.setInt(elem, i, j, k))

  def apply(a: Array[Array[Array[Long]]]) =
    make(a, TUInt, (data, elem: Long, i, j, k) => data.setUInt(elem, i, j, k))

  def apply(a: Array[Array[Array[Double]]]) =
    make(a, TValue(), (data, elem: Double, i, j, k) => data.setValue(elem, i, j, k))

  def apply(a: Array[Array[Array[Double]]], units: String) =
    make(a, TValue(units), (data, elem: Double, i, j, k) => data.setValue(elem, i, j, k))

  def apply(a: Array[Array[Array[String]]]) =
    make(a, TStr, (data, elem: String, i, j, k) => data.setString(elem, i, j, k))
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
  def apply(b: Boolean): Data = Data(TBool).setBool(b)
  def unapply(data: Data): Option[Boolean] =
    if (data.isBool) Some(data.getBool)
    else None
}

object UInt {
  def apply(l: Long): Data = Data(TUInt).setUInt(l)
  def unapply(data: Data): Option[Long] =
    if (data.isUInt) Some(data.getUInt)
    else None
}

object Integer {
  def apply(i: Int): Data = Data(TInt).setInt(i)
  def unapply(data: Data): Option[Int] =
    if (data.isInt) Some(data.getInt)
    else None
}

object Str {
  def apply(s: String): Data = Data(TStr).setString(s)
  def unapply(data: Data): Option[String] =
    if (data.isString) Some(data.getString)
    else None
}

object Bytes {
  def apply(s: Array[Byte]): Data = Data(TStr).setBytes(s)
  def unapply(data: Data): Option[Array[Byte]] =
    if (data.isBytes) Some(data.getBytes)
    else None
}

object Time {
  def apply(t: Date): Data = Data(TTime).setTime(t)
  def apply(t: DateTime): Data = Data(TTime).setTime(t)
  def apply(t: TimeStamp): Data = Data(TTime).setTime(t)
  def unapply(data: Data): Option[Date] =
    if (data.isTime) Some(data.getTime.toDate)
    else None
}

object Dbl {
  def apply(d: Double): Data = Data(TValue()).setValue(d)
  def unapply(data: Data): Option[Double] =
    if (data.isValue) Some(data.getValue)
    else None
}

object Value {
  def apply(v: Double): Data = Data(TValue()).setValue(v)
  def apply(v: Double, u: String): Data = Data(TValue(u)).setValue(v)
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
  def apply(re: Double, im: Double) = Data(TComplex()).setComplex(re, im)
  def apply(re: Double, im: Double, u: String): Data = Data(TComplex(u)).setComplex(re, im)
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
  def apply(code: Int, msg: String, payload: Data = Data.NONE): Data =
    Data(TError(payload.t)).setError(code, msg, payload)
  def apply(ex: Throwable): Data = ex match {
    case ex: LabradException => ex.toData
    case ex => apply(0, ex.toString)
  }
  def unapply(data: Data): Option[(Int, String, Data)] =
    if (data.isError) Some((data.getErrorCode, data.getErrorMessage, data.getErrorPayload))
    else None
}


// parsing data from string representation

object Parsers extends RegexParsers {

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
    ( "true" ^^ { _ => Bool(true) }
    | "false" ^^ { _ => Bool(false) }
    )

  def int: Parser[Data] =
    """[+-]\d+""".r ^^ { num => Integer(num.substring(if (num.startsWith("+")) 1 else 0).toInt) } // i8, i16, i64

  def uint: Parser[Data] =
    """\d+""".r ^^ { num => UInt(num.toLong) } // w8, w16, w64

  def string: Parser[Data] =
    ("\"" + """([^"\p{Cntrl}\\]|\\[\\/bfnrtv"]|\\x[a-fA-F0-9]{2})*""" + "\"").r ^^ { s => Bytes(Translate.stringToBytes(s)) }
    //("\""+"""([^"\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*"""+"\"").r

  def time: Parser[Data] =
    """\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(.\d{3})?Z""".r ^^ { s => Time(new DateTime(s).toDate) }

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
    ( "NaN" ^^ { _ => Double.NaN }
    | "Infinity" ^^ { _ => Double.PositiveInfinity }
    | """(\d*\.\d+|\d+(\.\d*)?)[eE][+-]?\d+""".r ^^ { _.toDouble }
    | """\d*\.\d+""".r ^^ { _.toDouble }
    )

  def complexNum: Parser[(Double, Double)] =
      signedReal ~ ("+" | "-") ~ unsignedReal <~ "i" ^^ {
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

  def termName = """[A-Za-z'"]+""".r

  def exponent =
      "^" ~ "-".? ~ number ~ ("/" ~ number).? ^^ {
        case carat ~ None    ~ n ~ None            => carat     + n
        case carat ~ None    ~ n ~ Some(slash ~ d) => carat     + n + slash + d
        case carat ~ Some(m) ~ n ~ None            => carat + m + n
        case carat ~ Some(m) ~ n ~ Some(slash ~ d) => carat + m + n + slash + d
      }

  def number = """\d+""".r

  def array = arrND ^^ { case (elems, typ, shape) =>
    val data = Data(TArr(typ, shape.size))
    data.setArrayShape(shape: _*)
    data.flatIterate((data, i) => data.set(elems(i)))
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

  private val translation = {
    // by default, use a two-digit hex escape code
    val temp = Array.tabulate[String](256) { b => """\x%02x""".format(b) }

    def translate(from: Byte, to: String): Unit = { temp((from + 256) % 256) = to }

    // printable ascii characters are not translated
    val ASCII_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 !@#$%^&*()-_=+|[]{}:;,.<>?/'"
    for (c <- ASCII_CHARS) translate(c.toByte, c.toString)

    // escape characters are preceded by a backslash
    translate('\n', """\n""")
    translate('\t', """\t""")
    translate('"',  """\"""")

    temp
  }

  def bytesToString(bytes: Array[Byte]): String =
    '"' + bytes.flatMap(b => translation((b + 256) % 256)).mkString + '"'

  def stringToBytes(s: String): Array[Byte] = {
    require(s.head == '"')
    require(s.last == '"')
    val trimmed = s.substring(1, s.length-1)
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
            case c => c.toByte
          }
        case c =>
          c.toByte
      })
      pos += 1
    }
    buf.result
  }
}


