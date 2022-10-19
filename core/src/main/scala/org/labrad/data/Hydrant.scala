package org.labrad.data

import java.util.Date
import org.labrad.types._
import scala.math
import scala.util.Random

// TODO: support generating random data from a pattern, not just a type
object Hydrant {

  private val random = new Random

  def randomType: Type = {
    def gen(nesting: Int, allowArray: Boolean = true): Type = {
      var options = Seq(Symbol("_"), Symbol("b"), Symbol("i"), Symbol("w"), Symbol("s"), Symbol("y"), Symbol("t"), Symbol("v"), Symbol("c"))
      if (nesting < 4) {
        if (allowArray) options ++= Seq(Symbol("array"))
        options ++= Seq(Symbol("cluster"))
      }
      val choice = options(random.nextInt(options.size))
      choice match {
        case Symbol("_") => TNone
        case Symbol("b") => TBool
        case Symbol("i") => TInt
        case Symbol("w") => TUInt
        case Symbol("s") => TStr
        case Symbol("y") => TBytes
        case Symbol("t") => TTime
        case Symbol("v") => TValue(randomUnits)
        case Symbol("c") => TComplex(randomUnits)

        case Symbol("cluster") =>
          val size = random.nextInt(5) + 1
          val elems = Seq.fill(size) { gen(nesting + 1) }
          TCluster(elems: _*)

        case Symbol("array") =>
          val nDims = random.nextInt(3) + 1
          val elem = gen(nesting + nDims, allowArray = false)
          TArr(elem, nDims)

        case _ =>
          sys.error("should not happen")
      }
    }
    gen(0)
  }

  def randomUnits = {
    // TODO: we could be a bit more sophisticated here
    val choices = Seq("", "s", "ms", "us", "m", "m/s", "V^2/Hz", "V/Hz^1/2")
    choices(random.nextInt(choices.size))
  }

  def randomData: Data = randomData(randomType)
  def randomData(s: String): Data = randomData(Type(s))

  def randomData(t: Type): Data = t match {
    case TNone => randomNone
    case TBool => randomBool
    case TInt => randomInt
    case TUInt => randomUInt
    case TStr => randomStr
    case TBytes => randomBytes
    case TTime => randomTime
    case TValue(units) => randomValue(units)
    case TComplex(units) => randomComplex(units)
    case t: TCluster => randomCluster(t)
    case t: TArr => randomArr(t)
    case TError(t) => randomError(t)
  }

  def randomNone = Data.NONE

  def randomBool = Bool(random.nextBoolean())

  def randomInt = Integer(random.nextInt())

  def randomUInt = UInt(random.nextInt(2000000000).toLong)

  def randomStr = {
    // return a random unicode character from the Basic Multilingual Plane (not a surrogate)
    def randomBMPChar: Char = {
      var c: Char = 0.toChar
      do {
        c = random.nextInt(0x10000).toChar
      } while (c.isSurrogate)
      c
    }
    val len = random.nextInt(100)
    val chars = Array.fill[Char](len) { randomBMPChar }
    Str(new String(chars))
  }

  def randomBytes = {
    val bytes = Array.ofDim[Byte](random.nextInt(100))
    random.nextBytes(bytes)
    Bytes(bytes)
  }

  def randomTime = {
    val time = System.currentTimeMillis + random.nextInt()
    Time(new Date(time))
  }

  def randomValue(units: Option[String]) = units match {
    case None => Value(nextDouble, randomUnits)
    case Some(units) => Value(nextDouble, units)
  }

  def randomComplex(units: Option[String]) = units match {
    case None => Cplx(nextDouble, nextDouble)
    case Some(units) => Cplx(nextDouble, nextDouble, units)
  }

  private def nextDouble = random.nextGaussian() * 1e9

  def randomCluster(t: TCluster) =
    Cluster(t.elems.map(randomData(_)): _*)

  def randomArr(t: TArr) = {
    val arr = Data(t)
    val emptyShape = Seq.tabulate(t.depth) { i => if (i < t.depth - 1) 1 else 0 }
    val shape = if (t.elem == TNone) {
      emptyShape
    } else {
      val shape = Seq.tabulate(t.depth) { i => random.nextInt(math.pow(2, 5 - t.depth).toInt) }
      val size = shape.product
      if (size == 0) emptyShape else shape
    }
    arr.setArrayShape(shape: _*)
    arr.flatIterator.foreach { elem =>
      elem.set(randomData(t.elem))
    }
    arr
  }

  def randomError(t: Type) = {
    val code = random.nextInt()
    val message = "random error"
    val payload = randomData(t)
    Error(code, message, payload)
  }
}
