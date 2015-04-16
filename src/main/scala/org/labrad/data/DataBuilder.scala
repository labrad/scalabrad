package org.labrad.data

import io.netty.buffer.{ByteBuf, Unpooled}
import java.nio.ByteOrder
import java.nio.ByteOrder._
import java.nio.charset.StandardCharsets.UTF_8
import org.labrad.types._

object DataBuilder {
  def apply(tag: String): DataBuilder = apply(Type(tag))
  def apply(t: Type)(implicit bo: ByteOrder = BIG_ENDIAN): DataBuilder = new DataBuilder(t)
}

class DataBuilder(t: Type)(implicit byteOrder: ByteOrder = BIG_ENDIAN) { self =>

  private val buf = Unpooled.buffer().order(byteOrder)

  private var state: State = Consume(t, caller = Done).call()

  override def toString: String = {
    s"DataBuilder($t, state = $state, bytes = ${buf.readableBytes})"
  }

  sealed trait State {
    def caller: State
    def call(): State
    def resume(): State

    override def toString: String = {
      var s = render("Done")
      var state = caller
      while (state != Done) {
        s = state.render(s)
        state = state.caller
      }
      s
    }
    def render(subState: String): String = super.toString
  }

  case object Done extends State {
    def caller: State = Done
    def call(): State = ???
    def resume(): State = Done
    override def render(subState: String): String = subState
  }

  private def Consume(t: Type, caller: State): State = {
    t match {
      case TArr(elem, depth) => ConsumeArray(elem, depth, caller)
      case TCluster(elems @ _*) => ConsumeCluster(elems, caller)
      case TError(payload) => Consume(TCluster(TInt, TStr, payload), caller)
      case t => ConsumeOne(t, caller)
    }
  }

  case class ConsumeOne(t: Type, caller: State) extends State {
    def call(): State = {
      if (t == TNone) caller.resume() else this
    }
    def resume(): State = ???
    def gotValue(): State = caller.resume()
    override def render(subState: String): String = s"<$t>"
  }

  case class ConsumeCluster(elems: Seq[Type], caller: State) extends State {
    private val consumers = elems.toArray.map(t => Consume(t, caller = this))
    private val size = elems.length
    private var i = 0

    def call(): State = {
      i = 0
      consumers(i).call()
    }

    def resume(): State = {
      i += 1
      if (i < size) {
        consumers(i).call()
      } else {
        caller.resume()
      }
    }

    override def render(subState: String): String = {
      val elemStrs = for ((elem, idx) <- elems.zipWithIndex) yield {
        if (idx == i) {
          subState
        } else {
          elem.toString
        }
      }
      s"(${elemStrs.mkString})"
    }
  }

  case class ConsumeArray(elem: Type, depth: Int, caller: State) extends State {
    private val consumer = Consume(elem, caller = this)
    private var shape: Array[Int] = null
    private var size: Int = 0
    private var i: Int = 0

    def call(): State = {
      shape = null
      i = 0
      this
    }

    def resume(): State = {
      i += 1
      if (i < size) {
        consumer.call()
      } else {
        caller.resume()
      }
    }

    def gotShape(shape: Array[Int]): State = {
      require(shape.length == depth, s"expecting shape of depth $depth, got ${shape.length}")
      require(shape.forall(_ >= 0), s"dimensions must be nonnegative, got ${shape.mkString(",")}")
      this.shape = shape
      this.size = shape.product
      for (dim <- shape) {
        buf.writeInt(dim)
      }
      consumer.call()
    }

    override def render(subState: String): String = {
      val dStr = if (depth == 1) "" else depth.toString
      if (shape == null) {
        s"*$dStr<shape>$elem"
      } else {
        s"*$dStr{$i}$subState"
      }
    }
  }

  private def nope(msg: String) = {
    throw new IllegalStateException(msg)
  }

  private def addSimple(p: Pattern)(write: => Unit): this.type = {
    state = state match {
      case state: ConsumeOne if p.accepts(state.t) => write; state.gotValue()
      case state: ConsumeOne => nope(s"cannot add $p. expecting ${state.t}")
      case state: ConsumeArray => nope(s"cannot add $p. expecting array shape")
      case state => nope(s"cannot add $t. unexpected state: $state")
    }
    this
  }

  def addBool(x: Boolean): this.type = addSimple(TBool) { buf.writeBoolean(x) }
  def addInt(x: Int): this.type = addSimple(TInt) { buf.writeInt(x) }
  def addUInt(x: Long): this.type = addSimple(TUInt) { buf.writeInt(x.toInt) }
  def addBytes(x: Array[Byte]): this.type = addSimple(TStr) { buf.writeInt(x.length); buf.writeBytes(x) }
  def addString(s: String): this.type = addSimple(TStr) {
    val bytes = s.getBytes(UTF_8)
    buf.writeInt(bytes.length)
    buf.writeBytes(bytes)
  }
  def addTime(seconds: Long, fraction: Long): this.type = addSimple(TTime) { buf.writeLong(seconds); buf.writeLong(fraction) }
  def addValue(x: Double): this.type = addSimple(PValue(None)) { buf.writeDouble(x) }
  def addComplex(re: Double, im: Double): this.type = {
    addSimple(PComplex(None)) { buf.writeDouble(re); buf.writeDouble(im) }
  }

  def setSize(size: Int): this.type = this.setShape(Array(size))
  def setShape(shape: Int*): this.type = this.setShape(shape.toArray)
  def setShape(shape: Array[Int]): this.type = {
    state = state match {
      case state: ConsumeArray => state.gotShape(shape)
      case state: ConsumeOne => nope(s"cannot set shape. expecting ${state.t}")
      case state => nope(s"cannot add $t. unexpected state: $state")
    }
    this
  }

  def build(): Data = {
    state match {
      case Done => new FlatData(t, buf.toByteArray, 0)
      case state: ConsumeArray => nope("cannot build. expecting array shape")
      case state: ConsumeOne => nope(s"cannot build. expecting ${state.t}")
      case state => nope(s"cannot build. unexpected state: $state")
    }
  }
}

