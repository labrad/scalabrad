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
package data

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, Serializable}
import java.nio.ByteOrder
import java.util.Date

import scala.collection._
import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.math

import errors.NonIndexableTypeException
import types._


case class ByteArrayView(bytes: Array[Byte], offset: Int)
case class Complex(real: Double, imag: Double)
case class Context(high: Long, low: Long) {
  def toData = Cluster(Word(high), Word(low))
}


case class TimeStamp(seconds: Long, fraction: Long)


trait IError {
  def code: Int
  def message: String
  def payload: IData
}

trait IBool {
  def toBool: Boolean
}

trait IInt {
  def toInt: Int
}

trait IWord {
  def toLong: Long
}

trait IBytes {
  def toBytes: Array[Byte]
}

trait IStr {
  def toStr: String
}

trait IArray extends IData {
  def apply(idx: Int): IData
  def size: Int
  def shape: Array[Int]
  
  def toSeq[T: ClassManifest](getter: IData => T): Seq[T] = t match {
    case TArr(_, 1) => Array.tabulate[T](size)(i => getter(this(i))).toSeq
    case _ => throw new Exception("Must be an array")
  }
  
  override def toBytes(implicit byteOrder: ByteOrder): Array[Byte] = {
    val os = new ByteArrayOutputStream
    for (dim <- shape) ByteManip.writeWord(os, dim)
    os.toByteArray
  }
}

trait ICluster extends IData {
  def apply(idx: Int): IData
  def size: Int
  
  override def toBytes(implicit byteOrder: ByteOrder): Array[Byte] =
    (0 until size).flatMap(this(_).toBytes).toArray
}

trait IData {
  def t: Type
    
  def approxEquals(other: Any): Boolean
  
  /** Flatten to array of bytes, suitable for sending over the wire. */
  def toBytes(implicit byteOrder: ByteOrder): Array[Byte]

  // type checks
  def isEmpty = t == TEmpty
  def isBool = t == TBool
  def isInt = t == TInteger
  //def isInt8: Boolean
  //def isInt16: Boolean
  //def isInt32: Boolean
  //def isInt64: Boolean
  def isWord = t == TWord
  //def isWord8: Boolean
  //def isWord16: Boolean
  //def isWord32: Boolean
  //def isWord64: Boolean
  def isBytes = t == TStr
  def isString = t == TStr
  def isValue = t.isInstanceOf[TValue]
  def isComplex = t.isInstanceOf[TComplex]
  def isTime = t == TTime
  def isArray = t.isInstanceOf[TArr]
  def isCluster = t.isInstanceOf[TCluster]
  def isError = t.isInstanceOf[TError]
  def hasUnits = t match {
    case TValue(u) => u != null
    case TComplex(u) => u != null
    case _ => false
  }
  
  // getters
  def getBool: Boolean
  def getInt: Int
  def getWord: Long
  def getBytes: Array[Byte]
  def getString: String
  def getString(enc: String): String
  def getValue: Double
  def getComplex: Complex
  def getUnits: String
  def getTime: Date
  def getTimeStamp: TimeStamp
  def getArrayShape: Array[Int]
  def getClusterSize: Int
  def getError: IError

  // setters
  def setBool(data: Boolean): Data
  def setInt(data: Int): Data
  def setWord(data: Long): Data
  def setBytes(data: Array[Byte]): Data
  def setString(data: String): Data
  def setString(data: String, encoding: String): Data
  def setValue(data: Double): Data
  def setComplex(data: Complex): Data
  def setComplex(re: Double, im: Double): Data
  def setTime(date: Date): Data
  def setTimeStamp(timestamp: TimeStamp): Data
  def setArraySize(size: Int): Data
  def setArrayShape(shape: Int*): Data
  def setError(code: Int, message: String): Data

  // indexed setters
  def setBool(data: Boolean, indices: Int*): Data
  def setInt(data: Int, indices: Int*): Data
  def setWord(data: Long, indices: Int*): Data
  def setBytes(data: Array[Byte], indices: Int*): Data
  def setString(data: String, indices: Int*): Data
  def setString(data: String, encoding: String, indices: Int*): Data
  def setValue(data: Double, indices: Int*): Data
  def setComplex(data: Complex, indices: Int*): Data
  def setComplex(re: Double, im: Double, indices: Int*): Data
  def setTime(date: Date, indices: Int*): Data
  def setArraySize(size: Int, indices: Int*): Data
  def setArrayShape(shape: Array[Int], indices: Int*): Data

  // array getters
  def getBoolArray: Array[Boolean]
  def getIntArray: Array[Int]
  def getWordArray: Array[Long]
  def getValueArray: Array[Double]
  def getStringArray: Array[String]
  def getDataArray: Array[Data]

  // vectorized getters
  //def getDataSeq: Seq[Data] = getDataArray.toSeq

  //def getBoolSeq = getSeq(_.getBool)
  //def getIntSeq = getSeq(_.getInt)
  //def getWordSeq = getSeq(_.getWord)
  //def getStringSeq = getSeq(_.getString)
  //def getDateSeq = getSeq(_.getTime)
  //def getDoubleSeq = getSeq(_.getValue)
  //def getComplexSeq = getSeq(_.getComplex)

  // vectorized indexed getters
  //  public List<Boolean> getBoolList(int...indices) { return get(indices).getBoolList(); }
  //  public List<Integer> getIntList(int...indices) { return get(indices).getIntList(); }
  //  public List<Long> getWordList(int...indices) { return get(indices).getWordList(); }
  //  public List<String> getStringList(int...indices) { return get(indices).getStringList(); }


  // vectorized setters
  def setSeq[T](data: Seq[T])(implicit setter: Setter[T]): Data
  def setBoolSeq(data: Seq[Boolean]) = setSeq(data)(Setters.boolSetter)
  def setIntSeq(data: Seq[Int]) = setSeq(data)(Setters.intSetter)
  def setWordSeq(data: Seq[Long]) = setSeq(data)(Setters.wordSetter)
  def setStringSeq(data: Seq[String]) = setSeq(data)(Setters.stringSetter)
  def setDateSeq(data: Seq[Date]) = setSeq(data)(Setters.dateSetter)
  def setDoubleSeq(data: Seq[Double]) = setSeq(data)(Setters.valueSetter)
  def setComplexSeq(data: Seq[Complex]) = setSeq(data)(Setters.complexSetter)


  // vectorized indexed setters
  def setBoolList(data: Seq[Boolean], indices: Int*): Data
  def setIntList(data: Seq[Int], indices: Int*): Data
  def setWordList(data: Seq[Long], indices: Int*): Data
  def setStringList(data: Seq[String], indices: Int*): Data
  def setDateList(data: Seq[Date], indices: Int*): Data
  def setDoubleList(data: Seq[Double], indices: Int*): Data
  def setComplexList(data: Seq[Complex], indices: Int*): Data
}

/**
 * The Data class encapsulates the data format used to communicate between
 * LabRAD servers and clients.  This data format is based on the
 * capabilities of LabVIEW, from National Instruments.  Each piece of LabRAD
 * data has a Type object which is specified by a String type tag.
 */
class Data private(val t: Type, data: Array[Byte], ofs: Int, heap: Buffer[Array[Byte]]) extends IData with Serializable with Cloneable {
  
  implicit val byteOrder = ByteOrder.BIG_ENDIAN
  
  /**
   * Construct a Data object for a given Type object.
   * 
   * @param tag
   *            the LabRAD Type of this Data object
   */
  protected def this(t: Type) =
    this(t, Data.createFilledByteArray(t.dataWidth), 0, Data.createHeap(t))
    
  /**
   * Make a copy of this Data object.
   */
  override def clone = Data.copy(this, new Data(this.t))

  /**
   * Get the LabRAD type tag string of this data object.
   * 
   * @return
   */
  def tag = t.toString

  override def equals(other: Any): Boolean =
    other match {
      case other: Data => Data.isEqual(this, other)
      case _ => false
    }
    
  def approxEquals(other: Any): Boolean =
    other match {
      case other: Data => Data.approxEqual(this, other)
      case _ => false
    }
  
  /**
   * Flatten LabRAD data into an array of bytes, suitable for sending over the wire.
   */
  def toBytes(implicit byteOrder: ByteOrder): Array[Byte] = {
    val os = new ByteArrayOutputStream
    toBytes(os, t, data, ofs, heap)
    os.toByteArray
  }
    
  /**
   * Flatten LabRAD data into the specified ByteArrayOutputStream.
   * 
   * Start flattening the specified buffer at some offset, using the given heap
   * for variable-length data chunks, and the Type object to specify how the
   * bytes are to be interpreted.
   * 
   * @param os
   * @param type
   * @param buf
   * @param ofs
   * @param heap
   * @throws IOException if writing to the output stream fails
   */
  private def toBytes(os: ByteArrayOutputStream, t: Type,
      buf: Array[Byte], ofs: Int, heap: Seq[Array[Byte]])(implicit byteOrder: ByteOrder) {
    if (t.fixedWidth && byteOrder == ByteOrder.BIG_ENDIAN) {
      os.write(buf, ofs, t.dataWidth)
    } else {
      t match {
        // handle fixed-width data structures for the case when endianness must be changed
        case TEmpty =>
          // no bytes to copy
        
        case TBool =>
          ByteManip.writeBool(os, ByteManip.getBool(buf, ofs))
          
        case TInteger =>
          ByteManip.writeInt(os, ByteManip.getInt(buf, ofs)(ByteOrder.BIG_ENDIAN))

        case TWord =>
          ByteManip.writeWord(os, ByteManip.getWord(buf, ofs)(ByteOrder.BIG_ENDIAN))

        case TValue(_) =>
          ByteManip.writeDouble(os, ByteManip.getDouble(buf, ofs)(ByteOrder.BIG_ENDIAN))

        case TComplex(_) =>
          ByteManip.writeComplex(os, ByteManip.getComplex(buf, ofs)(ByteOrder.BIG_ENDIAN))
          
        case TTime => // two longs
          ByteManip.writeLong(os, ByteManip.getLong(buf, ofs)(ByteOrder.BIG_ENDIAN))
          ByteManip.writeLong(os, ByteManip.getLong(buf, ofs + 8)(ByteOrder.BIG_ENDIAN))
          
        // handle variable-width data structures
        case TStr =>
          val sbuf = heap(ByteManip.getInt(buf, ofs)(ByteOrder.BIG_ENDIAN))
          ByteManip.writeInt(os, sbuf.length)
          os.write(sbuf)

        case TArr(elem, depth) =>
          // write list shape and compute total number of elements in the list
          var size = 1
          for (i <- 0 until depth) {
            val dim = ByteManip.getInt(buf, ofs + 4*i)(ByteOrder.BIG_ENDIAN)
            ByteManip.writeInt(os, dim)
            size *= dim
          }
          
          // write list data
          val lbuf = heap(ByteManip.getInt(buf, ofs + 4 * depth)(ByteOrder.BIG_ENDIAN))
          if (elem.fixedWidth && byteOrder == ByteOrder.BIG_ENDIAN) {
            // for fixed-width data, just copy in one big chunk
            os.write(lbuf, 0, elem.dataWidth * size)
          } else {
            // for variable-width data, flatten recursively
            val width = elem.dataWidth
            for (i <- 0 until size)
              toBytes(os, elem, lbuf, width * i, heap)
          }

        case t: TCluster =>
          for ((elem, delta) <- t.elems zip t.offsets)
            toBytes(os, elem, buf, ofs + delta, heap)

        case TError(payload) =>
          toBytes(os, TCluster(TInteger, TStr, payload), buf, ofs, heap)

        case _ =>
          throw new RuntimeException("Unknown type.")
      }
    }
  }
  
  override def toString = pretty //"Data(\"" + t.toString + "\")"

  /**
   * Returns a pretty-printed version of this LabRAD data.
   * 
   * @return
   */
  def pretty: String =
    t match {
      case TEmpty => ""
      
      case TBool =>
        getBool.toString
      
      case TInteger =>
        getInt match {
          case i if i >= 0 => "+" + i
          case i => i.toString
        }
        
      case TWord => getWord.toString

      case TValue(u) =>
        getValue.toString + u.map("[%s]".format(_)).getOrElse("")

      case TComplex(u) =>
        val c = getComplex
        c.real.toString + (if (c.imag >= 0) "+" else "-") +
        c.imag.toString + "i" + u.map(" [%s]".format(_)).getOrElse("")

      case TTime =>
        getTime.toString + " (" + getTime.getTime + ")"
      
      case TStr => '"' + (getString flatMap { char => char match {
          case 'a'|'b'|'c'|'d'|'e'|'f'|'g'|'h'|'i'|'j'|'k'|'l'|'m'|'n'|'o'|'p'|'q'|'r'|'s'|'t'|'u'|'v'|'w'|'x'|'y'|'z'|
               'A'|'B'|'C'|'D'|'E'|'F'|'G'|'H'|'I'|'J'|'K'|'L'|'M'|'N'|'O'|'P'|'Q'|'R'|'S'|'T'|'U'|'V'|'W'|'X'|'Y'|'Z'|
               '0'|'1'|'2'|'3'|'4'|'5'|'6'|'7'|'8'|'9'|
               ' '|'!'|'@'|'#'|'$'|'%'|'^'|'&'|'*'|'('|')'|'-'|'_'|'='|'+'|'|'|'+'|'['|']'|'{'|'}'|':'|';'|','|'.'|'<'|'>'|'?'|'/'
            => char.toString
          case '\n' => "\\n"
          case '\t' => "\\t"
          case char => "\\x%02x".format(char.asInstanceOf[Int])
        }}) + '"'

      case TArr(elem, depth) =>
        val shape = getArrayShape
        val indices = Array.ofDim[Int](depth)
        prettyList(shape, indices, 0)

      case t: TCluster =>
        val elems = for (i <- 0 until getClusterSize) yield this(i).pretty
        "(" + elems.mkString(", ") + ")"

      case TError(_) =>
        "Error(" + getErrorCode.toString + ", " + getErrorMessage + ")"

      case _ =>
        throw new RuntimeException("Unknown type: " + t + ".")
    }
  
  /**
   * Returns a pretty-printed version of a list object.
   * @param shape
   * @param indices
   * @param level
   * @return
   */
  private def prettyList(shape: Array[Int], indices: Array[Int], level: Int): String =
    if (shape(level) > 0) {
      val sb = new StringBuilder
      for (i <- 0 until shape(level)) {
        indices(level) = i
        if (i > 0) sb.append(", ")
        if (level == shape.length - 1)
          sb.append(this(indices: _*).pretty)
        else
          sb.append(prettyList(shape, indices, level + 1))
      }
      "[" + sb.toString + "]"
    } else {
      "[]"
    }

  /**
   * Extracts a subtype without typechecking.
   * @param indices
   * @return
   */
  private def subtype(indices: Int*): Type = {
    var t = this.t
    var dimsLeft = 0
    for (i <- indices) {
      t match {
        case TArr(elem, depth) =>
          if (dimsLeft == 0) dimsLeft = depth
          dimsLeft -= 1
          if (dimsLeft == 0) t = elem

        case TCluster(elems @ _*) =>
          t = elems(i)

        case _ =>
          throw new NonIndexableTypeException(t)
      }
    }
    if (dimsLeft != 0)
      throw new RuntimeException("Not enough indices for array.")
    t
  }

  private def offset: ByteArrayView = offset(Array.empty[Int]: _*)
  
  /**
   * Gets a view into the data array at the position specified by indices.
   * @param indices
   * @return
   */
  private def offset(indices: Int*) = {
    var t = this.t
    var data = this.data
    var dimsLeft = 0
    var shape = Array.empty[Int]
    var listIndices = Array.empty[Int]
    var ofs = this.ofs
    for (i <- indices) {
      t match {
        case TArr(elem, depth) =>
          if (dimsLeft == 0) {
            // read list shape
            shape = Array.ofDim[Int](depth)
            listIndices = Array.ofDim[Int](depth)
            for (j <- 0 until depth)
              shape(j) = ByteManip.getInt(data, ofs + 4 * j)
            dimsLeft = depth
            data = heap(ByteManip.getInt(data, ofs + 4 * depth))
          }
          // read one listIndex
          listIndices(depth - dimsLeft) = i
          dimsLeft -= 1
          if (dimsLeft == 0) {
            // set type to be element type of array
            t = elem
            ofs = 0
            // calculate offset into array
            var product = 1
            for (dim <- (depth - 1) to 0 by -1) {
              ofs += t.dataWidth * listIndices(dim) * product
              product *= shape(dim)
            }
          }

        case cluster: TCluster =>
          ofs += cluster.offsets(i)
          t = cluster.elems(i)

        case _ =>
          throw new NonIndexableTypeException(t)
      }
    }
    if (dimsLeft != 0) {
      throw new RuntimeException("Not enough indices for array.")
    }
    new ByteArrayView(data, ofs)
  }

  /**
   * Get a Data subobject at the specified list of indices.  Note that
   * this returns a view rather than a copy, so any modifications to
   * the subobject will be reflected in the original data.
   * @param indices
   * @return
   */
  //def get(List<Integer> indices) = {
  //  int[] indexArray = new int[indices.size]
  //  int i = 0
  //  for (int e : indices) indexArray[i++] = e
  //  get(indexArray)
  //}

  /**
   * Get a Data subobject at the specified array of indices.  Note that
   * this returns a view rather than a copy, so any modifications to
   * the subobject will be reflected in the original data.
   * @param indices
   * @return
   */
  def apply(indices: Int*) = {
    val t = subtype(indices: _*)
    val pos = offset(indices: _*)
    new Data(t, pos.bytes, pos.offset, heap);
  }

  /**
   * Set this data object based on the value of the other object.  In this case,
   * to prevent strangeness with shared heaps, the other object is copied into
   * this data object.
   * @param other
   * @return
   */
  def set(other: Data) {
    Data.copy(other, this)
    this
  }


  // getters
  def getBool = {
    require(isBool)
    ByteManip.getBool(offset)
  }

  def getInt = {
    require(isInt)
    ByteManip.getInt(offset)
  }

  def getWord = {
    require(isWord)
    ByteManip.getWord(offset)
  }

  def getBytes = {
    require(isBytes)
    heap(ByteManip.getInt(offset))
  }

  def getString = new String(getBytes, Data.STRING_ENCODING)
  def getString(enc: String) = new String(getBytes, enc)

  def getValue = {
    require(isValue)
    ByteManip.getDouble(offset)
  }

  def getComplex = {
    require(isComplex)
    ByteManip.getComplex(offset)
  }

  def getUnits = t match {
    case TValue(u) => u.getOrElse(null)
    case TComplex(u) => u.getOrElse(null)
    case _ => throw new Exception("Type " + t + " does not have units")
  }

  def getTime = {
    require(isTime)
    val ofs = offset
    var seconds = ByteManip.getLong(ofs.bytes, ofs.offset)
    var fraction = ByteManip.getLong(ofs.bytes, ofs.offset + 8)
    seconds -= Data.DELTA_SECONDS
    fraction = (fraction.toDouble / Long.MaxValue * 1000).asInstanceOf[Long]
    new Date(seconds * 1000 + fraction)
  }

  def getTimeStamp = {
    require(isTime)
    val ofs = offset
    var seconds = ByteManip.getLong(ofs.bytes, ofs.offset)
    var fraction = ByteManip.getLong(ofs.bytes, ofs.offset + 8)
    TimeStamp(seconds, fraction)
  }
  
  def getArraySize = {
    val shape = getArrayShape
    if (shape.length > 1)
      throw new Exception("Can't get size of multi-dimensional array.  Use getArrayShape.")
    shape(0)
  }

  def getArrayShape =
    t match {
      case TArr(_, depth) =>
        val pos = offset
        Array.tabulate(depth) { i =>
          ByteManip.getInt(pos.bytes, pos.offset + 4*i)
        }
      case _ => throw new Exception("Cannot get shape of non-array data")
    }

  def getClusterSize =
    t match {
      case TCluster(elems@_*) => elems.size
      case _ => throw new Exception("Cannot get cluster size of non-cluster data")
    }

  def getError = {
    require(isError)
    new IError {
      def code = getErrorCode
      def message = getErrorMessage
      def payload = getErrorPayload
    }
  }
  
  def getErrorCode = {
    require(isError)
    ByteManip.getInt(offset)
  }

  def getErrorMessage = {
    require(isError)
    val pos = offset
    val index = ByteManip.getInt(pos.bytes, pos.offset + 4)
    new String(heap(index), Data.STRING_ENCODING)
  }

  def getErrorPayload = {
    t match {
      case TError(payload) =>
        val pos = offset
        new Data(payload, pos.bytes, pos.offset + 8, heap)
      case _ => throw new Exception("Cannot get payload of non-error data")
    }
  }


  // indexed getters
  //    public boolean getBool(int...indices) { return get(indices).getBool(); }
  //    public int getInt(int...indices) { return get(indices).getInt(); }
  //    public long getWord(int...indices) { return get(indices).getWord(); }
  //    public byte[] getBytes(int...indices) { return get(indices).getBytes(); }
  //    public String getString(int...indices) { return get(indices).getString(); }
  //    public String getString(String encoding, int...indices)
  //            throws UnsupportedEncodingException {
  //        return get(indices).getString(encoding);
  //    }
  //    public double getValue(int...indices) { return get(indices).getValue(); }
  //    public Complex getComplex(int...indices) { return get(indices).getComplex(); }
  //    public String getUnits(int...indices) { return subtype(indices).getUnits(); }
  //    public Date getTime(int...indices) { return get(indices).getTime(); }
  //    public int getArraySize(int...indices) { return get(indices).getArraySize(); }
  //    public int[] getArrayShape(int...indices) { return get(indices).getArrayShape(); }
  //    public int getClusterSize(int...indices) {
  //        return subtype(Type.Code.CLUSTER, indices).size();
  //    }

  // setters
  def setBool(data: Boolean): Data = {
    require(isBool)
    ByteManip.setBool(offset, data)
    this
  }

  def setInt(data: Int): Data = {
    require(isInt)
    ByteManip.setInt(offset, data)
    this
  }

  def setWord(data: Long): Data = {
    require(isWord)
    ByteManip.setWord(offset, data)
    this
  }

  def setBytes(data: Array[Byte]): Data = {
    require(isString)
    val ofs = offset
    var heapLocation = ByteManip.getInt(ofs)
    if (heapLocation == -1) {
      // not yet set in the heap
      ByteManip.setInt(ofs, heap.size)
      heap += data
    } else
      // already set in the heap, reuse old spot
      heap(heapLocation) = data
    this
  }

  def setString(data: String): Data = {
    setBytes(data.getBytes(Data.STRING_ENCODING))
    this
  }

  def setString(data: String, encoding: String): Data =
    setBytes(data.getBytes(encoding))

  def setValue(data: Double): Data = {
    require(isValue)
    ByteManip.setDouble(offset, data)
    this
  }

  def setComplex(data: Complex): Data = {
    require(isComplex)
    ByteManip.setComplex(offset, data)
    this
  }

  def setComplex(re: Double, im: Double): Data =
    setComplex(new Complex(re, im))

  def setTime(date: Date): Data = {
    require(isTime)
    val millis = date.getTime
    val seconds = millis / 1000 + Data.DELTA_SECONDS
    var fraction = millis % 1000
    fraction = (fraction.toDouble / 1000 * Long.MaxValue).toLong
    val ofs = offset
    ByteManip.setLong(ofs.bytes, ofs.offset, seconds)
    ByteManip.setLong(ofs.bytes, ofs.offset + 8, fraction)
    this
  }

  def setTimeStamp(timestamp: TimeStamp) = {
    require(isTime)
    val ofs = offset
    ByteManip.setLong(ofs.bytes, ofs.offset, timestamp.seconds)
    ByteManip.setLong(ofs.bytes, ofs.offset + 8, timestamp.fraction)
    this
  }
  
  def setArraySize(size: Int) = {
    setArrayShape(Array(size): _*)
    this
  }

  def setArrayShape(shape: Int*) =
    t match {
      case TArr(elementType, depth) =>
        if (shape.length != depth)
          throw new RuntimeException("Array depth mismatch!")
        val pos = offset
        var size = 1
        for (i <- 0 until depth) {
          ByteManip.setInt(pos.bytes, pos.offset + 4*i, shape(i))
          size *= shape(i)
        }
        val buf = Data.createFilledByteArray(elementType.dataWidth * size)
        val heapIndex = ByteManip.getInt(pos.bytes, pos.offset + 4*depth)
        if (heapIndex == -1) {
          ByteManip.setInt(pos.bytes, pos.offset + 4*depth, heap.size)
          heap += buf
        } else {
          heap(heapIndex) = buf
        }
        this
      case _ => throw new Exception("Cannot set shape for non-array data")
    }

  def setError(code: Int, message: String) =
    t match {
      case TError(_) =>
        val pos = offset
        ByteManip.setInt(pos.bytes, pos.offset, code)
        val buf = message.getBytes(Data.STRING_ENCODING)
        val heapIndex = ByteManip.getInt(pos.bytes, pos.offset + 4)
        if (heapIndex == -1) {
          ByteManip.setInt(pos.bytes, pos.offset+4, heap.size)
          heap += buf
        } else
          heap(heapIndex) = buf
        this
      case _ => throw new Exception("Data type must be error")
    }


  // indexed setters
  def setBool(data: Boolean, indices: Int*): Data = {
    this(indices: _*).setBool(data)
    this
  }

  def setInt(data: Int, indices: Int*): Data = {
    this(indices: _*).setInt(data)
    this
  }

  def setWord(data: Long, indices: Int*): Data = {
    this(indices: _*).setWord(data)
    this
  }

  def setBytes(data: Array[Byte], indices: Int*): Data = {
    this(indices: _*).setBytes(data)
    this
  }

  def setString(data: String, indices: Int*): Data = {
    this(indices: _*).setString(data)
    this
  }

  def setString(data: String, encoding: String, indices: Int*): Data = {
    this(indices: _*).setString(data, encoding)
    this
  }

  def setValue(data: Double, indices: Int*): Data = {
    this(indices: _*).setValue(data)
    this
  }

  def setComplex(data: Complex, indices: Int*): Data = {
    this(indices: _*).setComplex(data)
    this
  }

  def setComplex(re: Double, im: Double, indices: Int*): Data = {
    setComplex(new Complex(re, im), indices: _*)
  }

  def setTime(date: Date, indices: Int*): Data = {
    this(indices: _*).setTime(date)
    this
  }

  def setArraySize(size: Int, indices: Int*): Data = {
    this(indices: _*).setArraySize(size)
    this
  }

  def setArrayShape(shape: Array[Int], indices: Int*): Data = {
    this(indices: _*).setArrayShape(shape: _*)
    this
  }

  //def setArrayShape(List<Integer> shape, int...indices) {
  //  get(indices).setArrayShape(shape);
  //  return this;
  //}

  // array getters
  def getBoolArray = t match {
    case TArr(TBool, 1) => Array.tabulate(getArraySize)(this(_).getBool)
    case _ => throw new Exception("Must have *b")
  }

  def getIntArray = t match {
    case TArr(TInteger, 1) => Array.tabulate(getArraySize)(this(_).getInt)
    case _ => throw new Exception("Must have *i")
  }

  def getWordArray = t match {
    case TArr(TWord, 1) => Array.tabulate(getArraySize)(this(_).getWord)
    case _ => throw new Exception("Must have *w")
  }

  def getValueArray = t match {
    case TArr(TValue(_), 1) => Array.tabulate(getArraySize)(this(_).getValue)
    case _ => throw new Exception("Must have *v")
  }

  def getStringArray = t match {
    case TArr(TStr, 1) => Array.tabulate(getArraySize)(this(_).getString)
    case _ => throw new Exception("Must have *s")
  }

  def getDataArray = t match {
    case TArr(_, 1) => Array.tabulate(getArraySize)(this(_))
    case _ => throw new Exception("Must be an array")
  }

  // vectorized getters
  def getDataSeq: Seq[Data] = getDataArray.toSeq

  def getSeq[T: ClassManifest](getter: Data => T): Seq[T] = t match {
    case TArr(_, 1) => Array.tabulate[T](getArraySize)(i => getter(this(i))).toSeq
    case _ => throw new Exception("Must be an array")
  }

  def getBoolSeq = getSeq(_.getBool)
  def getIntSeq = getSeq(_.getInt)
  def getWordSeq = getSeq(_.getWord)
  def getStringSeq = getSeq(_.getString)
  def getDateSeq = getSeq(_.getTime)
  def getDoubleSeq = getSeq(_.getValue)
  def getComplexSeq = getSeq(_.getComplex)

  // vectorized indexed getters
  //	public List<Boolean> getBoolList(int...indices) { return get(indices).getBoolList(); }
  //	public List<Integer> getIntList(int...indices) { return get(indices).getIntList(); }
  //	public List<Long> getWordList(int...indices) { return get(indices).getWordList(); }
  //	public List<String> getStringList(int...indices) { return get(indices).getStringList(); }


  // vectorized setters
  def setSeq[T](data: Seq[T])(implicit setter: Setter[T]) = t match {
    case TArr(elem, depth) if depth > 1 =>
      throw new Exception("must be a 1D array")
    case TArr(elem, 1) =>
      if (data.size > 0 && !setter.typ.accepts(elem)) throw new Exception("wrong element type")
      else {
        setArraySize(data.size)
        for (i <- 0 until data.size)
          setter.set(this(i), data(i))
        this
      }
    case _ =>
      throw new Exception("must be an array")
  }

  // vectorized indexed setters
  def setBoolList(data: Seq[Boolean], indices: Int*): Data = {
    this(indices: _*).setBoolList(data)
    this
  }

  def setIntList(data: Seq[Int], indices: Int*): Data = {
    this(indices: _*).setIntList(data)
    this
  }

  def setWordList(data: Seq[Long], indices: Int*): Data = {
    this(indices: _*).setWordList(data)
    this
  }

  def setStringList(data: Seq[String], indices: Int*): Data = {
    this(indices: _*).setStringList(data)
    this
  }

  def setDateList(data: Seq[Date], indices: Int*): Data = {
    this(indices: _*).setDateList(data)
    this
  }

  def setDoubleList(data: Seq[Double], indices: Int*): Data = {
    this(indices: _*).setDoubleList(data)
    this
  }

  def setComplexList(data: Seq[Complex], indices: Int*): Data = {
    this(indices: _*).setComplexList(data)
    this
  }
}

object Data {
  val STRING_ENCODING = "ISO-8859-1"
  val EMPTY = Data("")
  
  // time
  // TODO check timezones in time translation
  // LabRAD measures time as seconds and fractions of a second since Jan 1, 1904 GMT.
  // The Java Date class measures time as milliseconds since Jan 1, 1970 GMT.
  // The difference between these two is 24107 days.
  // 
  val DELTA_SECONDS = 24107L * 24L * 60L * 60L
  
  def apply(t: Type) = new Data(t)
  def apply(tag: String) = new Data(Type(tag))
  
  /**
   * Creates a byte array of the specified length filled with 0xff.
   * This is used to mark pointers into the heap so we know when we can
   * reuse heap addresses.  By initializing the byte array with 0xff,
   * all heap addresses will initially be set to -1, which is never
   * a valid heap index.
   * @param length of byte array to create
   * @return array of bytes initialized with 0xff
   */
  def createFilledByteArray(length: Int) = Array.fill[Byte](length)(0xFF.toByte)

  /**
   * Create a new heap object for data of the given type.  If the type in
   * question is fixed width, then no heap is needed, so we use an empty list.
   * @param type
   * @return
   */
  private def createHeap(t: Type) =
    if (t.fixedWidth)
      Buffer.empty[Array[Byte]]
    else
      ArrayBuffer.empty[Array[Byte]]
  
  /**
   * Copy Data object src to dest.
   * @param src
   * @param dest
   * @return
   */
  def copy(src: Data, dest: Data): Data = {
    src match {
      case Bool(b) => dest.setBool(b)
      case Integer(i) => dest.setInt(i)
      case Word(w) => dest.setWord(w)
      case Value(v, u) => dest.setValue(v) // TODO: units?
      case ComplexData(re, im, u) => dest.setComplex(re, im)
      case Time(t) => dest.setTime(t)
      case Bytes(s) => dest.setBytes(s)
      case NDArray(depth) =>
        val shape = src.getArrayShape
        val indices = Array.ofDim[Int](shape.length)
        dest.setArrayShape(shape)
        copyList(src, dest, shape, indices)

      case Cluster(_*) =>
        for (i <- 0 until src.getClusterSize)
          copy(src(i), dest(i))

      case Error(code, msg, payload) =>
        dest.setError(code, msg)
        // TODO add error payloads
        //clone.setPayload(src.getErrorPayload());

      case _ =>
        println(src.pretty)
        println(src.t)
        println(dest.t)
        throw new Exception("Not implemented!")
    }
    dest
  }

  /**
   * Copy a (possibly multidimensional) list from another Data object to this one.
   * @param other
   * @param shape
   * @param indices
   * @param level
   */
  private def copyList(src: Data, dest: Data, shape: Array[Int], indices: Array[Int], level: Int = 0) {
    for (i <- 0 until shape(level)) {
      indices(level) = i
      if (level == shape.length - 1)
        copy(src(indices: _*), dest(indices: _*))
      else
        copyList(src, dest, shape, indices, level + 1)
    }
  }

  
  def isEqual(src: Data, dest: Data): Boolean = src match {
    case Bool(b) => dest match {
      case Bool(`b`) => true
      case _ => false
    }
    case Integer(i) => dest match {
      case Integer(`i`) => true
      case _ => false
    }
    case Word(w) => dest match {
      case Word(`w`) => true
      case _ => false
    }
    case Value(v, u) => dest match {
      case Value(`v`, `u`) => true
      case _ => false
    }
    case ComplexData(re, im, u) => dest match {
      case ComplexData(`re`, `im`, `u`) => true
      case _ => false
    }
    case Time(t) => dest match {
      case Time(`t`) => true
      case _ => false
    }
    case Str(s) => dest match {
      case Str(`s`) => true 
      case _ => false
    }
    case NDArray(depth) => dest match {
      case NDArray(`depth`) =>
        val shape = src.getArrayShape
        val otherShape = dest.getArrayShape
        if (shape.toSeq == otherShape.toSeq) {
          val indices = Array.ofDim[Int](shape.length)
          equalsList(src, dest, shape, indices)
        } else {
          false
        }
      case _ => false
    }

    case Cluster(elems @ _*) => dest match {
      case Cluster(others @ _*) => elems == others
      case _ => false
    }

    case Error(code, msg, payload) => dest match {
      case Error(`code`, `msg`, `payload`) => true
      case _ => false
    }

    case _ =>
      println(src.pretty)
      println(src.t)
      println(dest.t)
      throw new Exception("Not implemented!")
  }
  
  private def equalsList(src: Data, other: Data, shape: Array[Int], indices: Array[Int], level: Int = 0): Boolean =
    (0 until shape(level)) forall { i =>
      indices(level) = i
      if (level == shape.length - 1)
        src(indices: _*) == other(indices: _*)
      else
        equalsList(src, other, shape, indices, level + 1)
    }
  
  
  def approxEqual(src: Data, dest: Data): Boolean = src match {
    case Value(v, u) => dest match {
      case Value(v2, u2) =>
        val close = approxEquals(v, v2)
        (u, u2) match {
          case (null, null) => close
          case (null, u2) => close
          case (u, null) => close
          case (u, u2) => close && u == u2
        }
      case _ => false
    }
    case ComplexData(re, im, u) => dest match {
      case ComplexData(re2, im2, u2) =>
        val close = approxEquals(re, re2) && approxEquals(im, im2)
        (u, u2) match {
          case (null, null) => close
          case (null, u2) => close
          case (u, null) => close
          case (u, u2) => close && u == u2
        }
      case _ => false
    }
    case Time(t) => dest match {
      case Time(t2) => math.abs(t.getTime - t2.getTime) <= 1
      case _ => false
    }
    case NDArray(depth) => dest match {
      case NDArray(`depth`) =>
        val shape = src.getArrayShape
        val otherShape = dest.getArrayShape
        if (shape.toSeq == otherShape.toSeq) {
          val indices = Array.ofDim[Int](shape.length)
          approxEqualsList(src, dest, shape, indices)
        } else {
          false
        }
      case _ => false
    }

    case Cluster(elems @ _*) => dest match {
      case Cluster(others @ _*) if others.size == elems.size =>
        (elems zip others) forall { case (elem, other) => elem approxEquals other }
      case _ => false
    }

    case _ => src == dest
  }
  
  private def approxEquals(a: Double, b: Double, tol: Double = 1e-9): Boolean = {
    val denom = math.max(math.abs(a), math.abs(b)) match {
      case 0 => 1
      case x => x
    }
    math.abs((a - b) / denom) < tol
  }
  
  private def approxEqualsList(src: Data, other: Data, shape: Array[Int], indices: Array[Int], level: Int = 0): Boolean =
    (0 until shape(level)) forall { i =>
      indices(level) = i
      if (level == shape.length - 1)
        src(indices: _*) approxEquals other(indices: _*)
      else
        equalsList(src, other, shape, indices, level + 1)
    }
  
  
  
  // static constructors for building clusters from groups of data objects
  /**
   * Build a cluster from an array of other data objects.
   * @param elements
   * @return
   */
  def clusterOf(elements: Data*): Data = {
    val elementTypes = for (elem <- elements) yield elem.t
    val cluster = new Data(TCluster(elementTypes: _*))
    for (i <- 0 until elementTypes.size)
      cluster(i).set(elements(i))
    cluster
  }
  
  def listOf(elements: Data*): Data = {
    val elementType = if (elements.size == 0) TEmpty else elements(0).t
    val data = new Data(TArr(elementType))
    data.setArraySize(elements.size)
    var i = 0
    for (elem <- elements) {
      data(i).set(elem)
      i += 1
    }
    data
  }
  
  def listOf[T](elements: Seq[T])(implicit setter: Setter[T]) = {
    val elementType = if (elements.size == 0) TAny else setter.typ
    val data = new Data(TArr(elementType))
    data.setSeq(elements)
    data
  }
  
  // static constructors for basic types
  def valueOf(b: Boolean) = Data("b").setBool(b)
  def valueOf(i: Int) = Data("i").setInt(i)
  def valueOf(w: Long) = Data("w").setWord(w)
  def valueOf(b: Array[Byte]) = Data("s").setBytes(b)
  def valueOf(s: String) = Data("s").setString(s)
  def valueOf(t: Date) = Data("t").setTime(t)
  def valueOf(v: Double) = Data("v").setValue(v)
  def valueOf(v: Double, units: String) = Data("v[" + units + "]").setValue(v)
  def valueOf(re: Double, im: Double) = Data("c").setComplex(re, im)
  def valueOf(re: Double, im: Double, units: String) = Data("c[" + units + "]").setComplex(re, im)

  // static constructors for arrays of basic types
  def valueOf(a: Array[Boolean]) = {
    val data = ofType("*b")
    data.setArraySize(a.length)
    for (i <- 0 until a.length)
      data.setBool(a(i), i)
    data
  }

  def valueOf(a: Array[Int]) = {
    val data = ofType("*i")
    data.setArraySize(a.length)
    for (i <- 0 until a.length)
      data.setInt(a(i), i)
    data
  }

  def valueOf(a: Array[Long]) = {
    val data = ofType("*w")
    data.setArraySize(a.length)
    for (i <- 0 until a.length)
      data.setWord(a(i), i)
    data
  }

  def valueOf(a: Array[Double]) = {
    val data = ofType("*v")
    data.setArraySize(a.length)
    for (i <- 0 until a.length)
      data.setValue(a(i), i)
    data
  }

  def valueOf(a: Array[Double], units: String) = {
    val data = ofType("*v[" + units + "]")
    data.setArraySize(a.length)
    for (i <- 0 until a.length)
      data.setValue(a(i), i)
    data
  }

  def valueOf(a: Array[String]) = {
    val data = ofType("*s")
    data.setArraySize(a.length)
    for (i <- 0 until a.length)
      data.setString(a(i), i)
    data
  }
  
  /*
  public static Data valueOf(Date[] t) {
    return new Data("t").setTime(t);
  }

  public static Data valueOf(double[] re, double[] im) {
    return new Data("c").setComplex(re, im);
  }
  public static Data valueOf(double[] re, double[] im, String units) {
    return new Data("c[" + units + "]").setComplex(re, im);
  }
  */

  //static constructors for 2D arrays of basic types
  // TODO ensure that 2D and 3D arrays are rectangular
  private def arr2[T](a: Array[Array[T]], elemType: Type, setter: (Data, T, Int, Int) => Unit) = {
    val data = Data.ofType(TArr(elemType, 2))
    val (lim0, lim1) = (a.length, (if (a.length > 0) a(0).length else 0))
    data.setArrayShape(lim0, lim1)
    for (i <- 0 until lim0; j <- 0 until lim1)
      setter(data, a(i)(j), i, j)
    data
  }
  
  private def arr3[T](a: Array[Array[Array[T]]], elemType: Type, setter: (Data, T, Int, Int, Int) => Unit) = {
    val data = Data.ofType(TArr(elemType, 3))
    val (lim0, lim1, lim2) = (a.length,
                              if (a.length > 0) a(0).length else 0,
                              if (a.length > 0 && a(0).length > 0) a(0)(0).length else 0)
    data.setArrayShape(lim0, lim1, lim2)
    for (i <- 0 until lim0; j <- 0 until lim1; k <- 0 until lim2)
      setter(data, a(i)(j)(k), i, j, k)
    data
  }
  
  def arr(a: Array[Array[Boolean]]) =
    arr2(a, TBool, (data, elem: Boolean, i, j) => data.setBool(elem, i, j))
  
  def arr(a: Array[Array[Int]]) =
    arr2(a, TInteger, (data, elem: Int, i, j) => data.setInt(elem, i, j))

  def arr(a: Array[Array[Long]]) =
    arr2(a, TWord, (data, elem: Long, i, j) => data.setWord(elem, i, j))

  def arr(a: Array[Array[Double]]) =
    arr2(a, TValue(), (data, elem: Double, i, j) => data.setValue(elem, i, j))
  
  def arr(a: Array[Array[Double]], units: String) =
    arr2(a, TValue(units), (data, elem: Double, i, j) => data.setValue(elem, i, j))

  def arr(a: Array[Array[String]]) =
    arr2(a, TStr, (data, elem: String, i, j) => data.setString(elem, i, j))
    
  
  //static constructors for 3D arrays of basic types
  def arr(a: Array[Array[Array[Boolean]]]) =
    arr3(a, TBool, (data, elem: Boolean, i, j, k) => data.setBool(elem, i, j, k))

  def arr(a: Array[Array[Array[Int]]]) =
    arr3(a, TInteger, (data, elem: Int, i, j, k) => data.setInt(elem, i, j, k))

  def arr(a: Array[Array[Array[Long]]]) =
    arr3(a, TWord, (data, elem: Long, i, j, k) => data.setWord(elem, i, j, k))

  def arr(a: Array[Array[Array[Double]]]) =
    arr3(a, TValue(), (data, elem: Double, i, j, k) => data.setValue(elem, i, j, k))

  def arr(a: Array[Array[Array[Double]]], units: String) =
    arr3(a, TValue(units), (data, elem: Double, i, j, k) => data.setValue(elem, i, j, k))

  def arr(a: Array[Array[Array[String]]]) =
    arr3(a, TStr, (data, elem: String, i, j, k) => data.setString(elem, i, j, k))
  
  
  // static constructors for specific types
  def ofType(tag: String) = new Data(Type(tag))
  def ofType(t: Type) = new Data(t)

  /**
   * Unflatten bytes from the specified buffer into Data, according to the Type.
   * 
   * @param buf
   * @param type
   * @return
   * @throws IOException
   */
  def fromBytes(buf: Array[Byte], t: Type)(implicit order: ByteOrder): Data =
    fromBytes(new ByteArrayInputStream(buf), t)

  /**
   * Unflatten a Data object from the given input stream of bytes.
   * 
   * @param is
   * @param type
   * @return
   * @throws IOException
   */
  def fromBytes(is: ByteArrayInputStream, t: Type)(implicit order: ByteOrder): Data = {
    val data = Array.ofDim[Byte](t.dataWidth)
    val heap = createHeap(t)
    fromBytes(is, t, data, 0, heap)
    new Data(t, data, 0, heap)
  }

  /**
   * Unflatten from a stream of bytes according to type, into the middle
   * of a Data object, as specified by the byte buffer, offset, and heap.
   * 
   * @param is
   * @param type
   * @param buf
   * @param ofs
   * @param heap
   * @throws IOException
   */
  private def fromBytes(is: ByteArrayInputStream,
      t: Type, buf: Array[Byte], ofs: Int, heap: Buffer[Array[Byte]])(implicit order: ByteOrder) {
    if (t.fixedWidth && order == ByteOrder.BIG_ENDIAN)
      is.read(buf, ofs, t.dataWidth)
    else
      t match {
        // handle fixed-width cases when byte order needs to be switched
        
        case TEmpty =>
          // no bytes to copy
      
        case TBool =>
          ByteManip.setBool(buf, ofs, ByteManip.readBool(is))
          
        case TInteger =>
          ByteManip.setInt(buf, ofs, ByteManip.readInt(is))(ByteOrder.BIG_ENDIAN)

        case TWord =>
          ByteManip.setWord(buf, ofs, ByteManip.readWord(is))(ByteOrder.BIG_ENDIAN)

        case TValue(_) =>
          ByteManip.setDouble(buf, ofs, ByteManip.readDouble(is))(ByteOrder.BIG_ENDIAN)

        case TComplex(_) =>
          ByteManip.setComplex(buf, ofs, ByteManip.readComplex(is))(ByteOrder.BIG_ENDIAN)
          
        case TTime => // two longs
          ByteManip.setLong(buf, ofs, ByteManip.readLong(is))(ByteOrder.BIG_ENDIAN)
          ByteManip.setLong(buf, ofs + 8, ByteManip.readLong(is))(ByteOrder.BIG_ENDIAN)
      
        case TStr =>
          val len = ByteManip.readInt(is)
          val sbuf = Array.ofDim[Byte](len)
          ByteManip.setInt(buf, ofs, heap.size)(ByteOrder.BIG_ENDIAN)
          heap += sbuf
          is.read(sbuf, 0, len)

        case TArr(elem, depth) =>
          val elementWidth = elem.dataWidth
          var size = 1
          for (i <- 0 until depth) {
            val dim = ByteManip.readInt(is)
            ByteManip.setInt(buf, ofs + 4 * i, dim)(ByteOrder.BIG_ENDIAN)
            size *= dim
          }
          val lbuf = Array.ofDim[Byte](elementWidth * size)
          ByteManip.setInt(buf, ofs + 4 * depth, heap.size)(ByteOrder.BIG_ENDIAN)
          heap += lbuf
          if (elem.fixedWidth && order == ByteOrder.BIG_ENDIAN)
            is.read(lbuf, 0, elementWidth * size)
          else
            for (i <- 0 until size)
              fromBytes(is, elem, lbuf, elementWidth * i, heap)

        case t: TCluster =>
          for ((elem, delta) <- t.elems zip t.offsets)
            fromBytes(is, elem, buf, ofs + delta, heap)

        case TError(payload) =>
          val tag = "is" + payload.toString
          fromBytes(is, Type(tag), buf, ofs, heap)
          //fromBytes(is, TCluster(TInteger, TStr, payload), buf, ofs, heap)

        case _ =>
          throw new RuntimeException("Unknown type: " + t)
      }
  }
}


// incoming data: stored as byte arrays; need ways to pattern match on it, and unpack it into java data


// extractors for pattern-matching labrad data

object Cluster {
  def apply(elems: Data*) = Data.clusterOf(elems: _*)
  def unapplySeq(data: Data): Option[Seq[Data]] =
    if (data.isCluster) {
      val array = Array.tabulate[Data](data.getClusterSize) { i => data(i) }
      Some(array.toSeq)
    }
    else None
}


object Arr {
  def apply(elems: Seq[Data]) = Data.listOf(elems: _*)
  def apply(elem: Data, elems: Data*) = Data.listOf((elem +: elems): _*)
  def apply[T](elems: T*)(implicit setter: Setter[T]) = Data.listOf(elems)
  def unapplySeq(data: Data): Option[Seq[Data]] =
    if (data.isArray) Some(data.getDataArray)
    else None
}


object NDArray {
  def unapply(data: Data): Option[Int] = data.t match {
    case TArr(_, depth) => Some(depth)
    case _ => None
  }
}


object Bool {
  def apply(b: Boolean) = Data.valueOf(b)
  def unapply(data: Data): Option[Boolean] =
    if (data.isBool) Some(data.getBool)
    else None
}


object Word {
  def apply(l: Long) = Data.valueOf(l)
  def unapply(data: Data): Option[Long] =
    if (data.isWord) Some(data.getWord)
    else None
}


object Integer {
  def apply(i: Int) = Data.valueOf(i)
  def unapply(data: Data): Option[Int] =
    if (data.isInt) Some(data.getInt)
    else None
}


object Val {
  def apply(d: Double) = Data.valueOf(d)
  def unapply(data: Data): Option[Double] =
    if (data.isValue) Some(data.getValue)
    else None
}


object Str {
  def apply(s: String) = Data.valueOf(s)
  def unapply(data: Data): Option[String] =
    if (data.isString) Some(data.getString)
    else None
}


object Bytes {
  def apply(s: Array[Byte]): Data = Data.valueOf(s)
  def unapply(data: Data): Option[Array[Byte]] =
    if (data.isBytes) Some(data.getBytes)
    else None
}


object Time {
  def apply(t: Date): Data = Data.valueOf(t)
  def unapply(data: Data): Option[Date] =
    if (data.isTime) Some(data.getTime)
    else None
}

object Value {
  def apply(v: Double): Data = Data.valueOf(v)
  def apply(v: Double, u: String): Data = Data.valueOf(v, u)
  def unapply(data: Data): Option[(Double, String)] =
    if (data.isValue) Some((data.getValue, data.getUnits))
    else None
}

object ComplexData {
  def apply(re: Double, im: Double) = Data.valueOf(re, im)
  def apply(re: Double, im: Double, u: String): Data = Data.valueOf(re, im, u)
  def unapply(data: Data): Option[(Double, Double, String)] =
    if (data.isComplex) Some((data.getComplex.real, data.getComplex.imag, data.getUnits))
    else None
}

object Error {
  // TODO: support payload data in error
  def apply(code: Int, msg: String, payload: Data = Data.EMPTY) = Data.ofType("E").setError(code, msg)
  def unapply(data: Data): Option[(Int, String, Data)] =
    if (data.isError) Some((data.getErrorCode, data.getErrorMessage, data.getErrorPayload))
    else None
}
