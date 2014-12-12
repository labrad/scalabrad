package org.labrad
package data

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
import scala.reflect.ClassTag
import scala.util.parsing.combinator.RegexParsers

class ShapeIterator(shape: Array[Int]) extends Iterator[Array[Int]] {
  private val nDims = shape.size
  private val indices = Array.ofDim[Int](nDims)
  indices(nDims - 1) = -1

  private var _hasNext = shape.forall(_ > 0)

  def hasNext: Boolean = _hasNext

  def next: Array[Int] = {
    // increment indices
    var k = nDims - 1
    while (k >= 0) {
      indices(k) += 1
      if (indices(k) == shape(k)) {
        indices(k) = 0
        k -= 1
      } else {
        k = -1 // done
      }
    }
    // check if this is the last iteration
    var last = true
    k = nDims - 1
    while (k >= 0) {
      if (indices(k) == shape(k) - 1) {
        k -= 1
      } else {
        last = false
        k = -1 // done
      }
    }
    _hasNext = !last
    indices
  }
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

/**
 * The Data class encapsulates the data format used to communicate between
 * LabRAD servers and clients.  This data format is based on the
 * capabilities of LabVIEW, from National Instruments.  Each piece of LabRAD
 * data has a Type object which is specified by a String type tag.
 */
class Data protected(val t: Type, buf: Array[Byte], ofs: Int, heap: Buffer[Array[Byte]])(implicit byteOrder: ByteOrder)
extends Serializable with Cloneable {

  import RichByteArray._ // implicitly add endian-aware get* and set* methods to Array[Byte]

  override def clone = Data.copy(this, Data(this.t))

  override def equals(other: Any): Boolean = other match {
    case other: Data => Data.isEqual(this, other)
    case _ => false
  }

  def ~==(other: Any): Boolean = other match {
    case other: Data => Data.approxEqual(this, other)
    case _ => false
  }

  /**
   * Create a new Data object that reinterprets the current data with a new type.
   */
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
      s"Error($getErrorCode, $getErrorMessage, $getErrorPayload)"
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
            data => data.flatIterator.foreach { func }
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

  /**
   * Get a Data subobject at the specified array of indices.  Note that
   * this returns a view rather than a copy, so any modifications to
   * the subobject will be reflected in the original data.
   */
  def apply(idx: Int*): Data = {
    @tailrec
    def find(t: Type, buf: Array[Byte], ofs: Int, idx: Seq[Int]): Data =
      if (idx.isEmpty)
        new Data(t, buf, ofs, heap)
      else
        t match {
          case TArr(elem, depth) =>
            if (idx.size < depth)
              sys.error("not enough indices for array")
            val arrBuf = heap(buf.getInt(ofs + 4 * depth))
            var arrOfs = 0
            var stride = 1
            val shape = Seq.tabulate(depth)(i => buf.getInt(ofs + 4 * i))
            for (dim <- (depth - 1) to 0 by -1) {
              arrOfs += elem.dataWidth * idx(dim) * stride
              stride *= shape(dim)
            }
            find(elem, arrBuf, arrOfs, idx.drop(depth))

          case t: TCluster =>
            find(t.elems(idx.head), buf, ofs + t.offsets(idx.head), idx.tail)

          case _ =>
            sys.error("type " + t + " is not indexable")
        }
    find(this.t, this.buf, this.ofs, idx)
  }

  /** Return an iterator that runs over all the elements in an N-dimensional array */
  def flatIterator: Iterator[Data] = t match {
    case TArr(elem, depth) =>
      val size = arrayShape.product
      val arrBuf = heap(buf.getInt(ofs + 4 * depth))
      Iterator.tabulate(size) { i =>
        new Data(elem, arrBuf, i * elem.dataWidth, heap)
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

  def setArrayShape(shape: Array[Int]): Data = t match {
    case TArr(elem, depth) =>
      require(shape.length == depth)
      val size = shape.product
      for (i <- 0 until depth)
        buf.setInt(ofs + 4*i, shape(i))
      val newBuf = Data.newByteArray(elem.dataWidth * size)
      val heapIndex = buf.getInt(ofs + 4*depth)
      if (heapIndex == -1) {
        buf.setInt(ofs + 4*depth, heap.size)
        heap += newBuf
      } else {
        val oldBuf = heap(heapIndex)
        Array.copy(oldBuf, 0, newBuf, 0, oldBuf.size min newBuf.size)
        heap(heapIndex) = newBuf
      }
      this
    case _ =>
      sys.error("arrayShape can only be set for arrays")
  }
  def setArrayShape(shape: Int*): Data = setArrayShape(shape.toArray)

  def setArraySize(size: Int): Data = setArrayShape(size)

  def clusterSize: Int = t match {
    case TCluster(elems @ _*) => elems.size
    case _ => sys.error("clusterSize is only defined for clusters")
  }

  // getters
  def getBool: Boolean = { require(isBool); buf.getBool(ofs) }
  def getInt: Int = { require(isInt); buf.getInt(ofs) }
  def getUInt: Long = { require(isUInt); buf.getUInt(ofs) }
  def getBytes: Array[Byte] = { require(isBytes); heap(buf.getInt(ofs)) }
  def getString: String = new String(getBytes, UTF_8)
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

  def setComplex(re: Double, im: Double): Data = {
    require(isComplex)
    buf.setDouble(ofs, re)
    buf.setDouble(ofs + 8, im)
    this
  }
  def setComplex(c: Complex): Data = setComplex(c.real, c.imag)

  def setTime(seconds: Long, fraction: Long): Data = {
    require(isTime)
    buf.setLong(ofs, seconds)
    buf.setLong(ofs + 8, fraction)
    this
  }
  def setTime(timestamp: TimeStamp): Data = setTime(timestamp.seconds, timestamp.fraction)
  def setTime(date: Date): Data = setTime(TimeStamp(date))
  def setTime(dateTime: DateTime): Data = setTime(TimeStamp(dateTime))

  def setError(code: Int, message: String, payload: Data = Data.NONE) = t match {
    case TError(payloadType) =>
      val data = cast(TCluster(TInt, TStr, payloadType))
      data(0).setInt(code)
      data(1).setString(message)
      data(2).set(payload)
      this
    case _ => sys.error("Data type must be error")
  }

  def get[T](implicit getter: Getter[T]): T = getter.get(this)
}

object Data {
  val NONE = Data("")

  import RichByteArray._

  def apply(tag: String): Data = apply(Type(tag))
  def apply(t: Type)(implicit bo: ByteOrder = BIG_ENDIAN): Data =
    new Data(t, newByteArray(t.dataWidth), 0, newHeap)

  /**
   * Parse data from string representation
   */
  def parse(data: String): Data = Parsers.parseData(data)

  private[data] def newByteArray(length: Int): Array[Byte] = Array.fill[Byte](length)(0xFF.toByte)
  private[data] def newHeap: Buffer[Array[Byte]] = Buffer.empty[Array[Byte]]

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
      case TStr => dst.setBytes(src.getBytes)

      case TArr(elem, depth) =>
        val shape = src.arrayShape
        dst.setArrayShape(shape)
        for ((srcElem, dstElem) <- src.flatIterator zip dst.flatIterator) {
          copy(srcElem, dstElem)
        }

      case TCluster(elems @ _*) =>
        for (i <- 0 until elems.size) {
          copy(src(i), dst(i))
        }

      case TError(_) =>
        dst.setError(src.getErrorCode, src.getErrorMessage, src.getErrorPayload)
    }
    dst
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
        (shape.toSeq == dst.arrayShape.toSeq) &&
          (src.flatIterator zip dst.flatIterator).forall { case (a, b) => a == b }
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
        shape.toSeq == dst.arrayShape.toSeq &&
          (src.flatIterator zip dst.flatIterator).forall { case (a, b) => a ~== b }
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
      data(i).set(elem)
    data
  }
  def unapplySeq(data: Data): Option[Seq[Data]] = data.t match {
    case TCluster(_*) => Some(Seq.tabulate(data.clusterSize)(data(_)))
    case _ => None
  }
}

object Arr {
  private def make[T](a: Array[T], elemType: Type)(implicit setter: Setter[T]) = {
    val data = Data(TArr(elemType, 1))
    val m = a.length
    data.setArrayShape(m)
    for (i <- 0 until m) {
      setter.set(data(i), a(i))
    }
    data
  }

  def apply(elems: Array[Data]): Data = {
    val elemType = if (elems.size == 0) TNone else elems(0).t
    make[Data](elems, elemType)
  }
  def apply(elems: Seq[Data]): Data = apply(elems.toArray)
  def apply(elem: Data, elems: Data*): Data = apply(elem +: elems)

  def apply(a: Array[Boolean]): Data = make[Boolean](a, TBool)
  def apply(a: Array[Int]): Data = make[Int](a, TInt)
  def apply(a: Array[Long]): Data = make[Long](a, TUInt)
  def apply(a: Array[String]): Data = make[String](a, TStr)
  def apply(a: Array[Double]): Data = make[Double](a, TValue())
  def apply(a: Array[Double], units: String) = make[Double](a, TValue(units))

  def unapplySeq(data: Data): Option[Seq[Data]] =
    if (data.isArray) Some(data.get[Array[Data]])
    else None
}

object Arr2 {
  private def make[T](a: Array[Array[T]], elemType: Type)(implicit setter: Setter[T]) = {
    val data = Data(TArr(elemType, 2))
    val (m, n) = (a.length, if (a.length > 0) a(0).length else 0)
    data.setArrayShape(m, n)
    for (i <- 0 until m) {
      assert(a(i).length == a(0).length, "array must be rectangular")
      for (j <- 0 until n)
        setter.set(data(i, j), a(i)(j))
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
  private def make[T](a: Array[Array[Array[T]]], elemType: Type)(implicit setter: Setter[T]) = {
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
          setter.set(data(i, j, k), a(i)(j)(k))
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

  private val translation = {
    // by default, use a two-digit hex escape code
    val lut = Array.tabulate[String](256) { b => """\x%02x""".format(b) }

    def translate(from: Byte, to: String): Unit = { lut((from + 256) % 256) = to }

    // printable ascii characters are not translated
    val ASCII_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 !@#$%^&*()-_=+|[]{}:;,.<>?/'"
    for (c <- ASCII_CHARS) translate(c.toByte, c.toString)

    // escape characters are preceded by a backslash
    translate('\n', """\n""")
    translate('\t', """\t""")
    translate('"',  """\"""")

    lut
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


