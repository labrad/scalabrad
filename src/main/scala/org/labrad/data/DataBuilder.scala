package org.labrad.data

import io.netty.buffer.{ByteBuf, Unpooled}
import java.nio.ByteOrder
import java.nio.ByteOrder._
import java.nio.charset.StandardCharsets.UTF_8
import org.labrad.types._
import scala.collection.mutable

object DataBuilder {
  def apply(tag: String): DataBuilder = apply(Type(tag))
  def apply(t: Type = null)(implicit bo: ByteOrder = BIG_ENDIAN): DataBuilder = new DataBuilder(Option(t))
}

class DataBuilder(tOpt: Option[Type] = None)(implicit byteOrder: ByteOrder = BIG_ENDIAN) { self =>

  private val buf = Unpooled.buffer().order(byteOrder)
  private var state: State = Start(tOpt).call()

  def none(): this.type = { state = state.none(); this }
  def bool(x: Boolean): this.type = { state = state.bool(x); this }
  def int(x: Int): this.type = { state = state.int(x); this }
  def uint(x: Long): this.type = { state = state.uint(x); this }
  def bytes(x: Array[Byte]): this.type = { state = state.bytes(x); this }
  def string(s: String): this.type = { state = state.string(s); this }
  def time(seconds: Long, fraction: Long): this.type = { state = state.time(seconds, fraction); this }
  def value(x: Double, unit: String = ""): this.type = { state = state.value(x, unit); this }
  def complex(re: Double, im: Double, unit: String = ""): this.type = { state = state.complex(re, im, unit); this }

  def array(size: Int): this.type = array(Array(size))
  def array(shape: Int*): this.type = array(shape.toArray)
  def array(shape: Array[Int]): this.type = { state = state.array(shape); this }

  def clusterStart(): this.type = { state = state.clusterStart(); this }
  def clusterEnd(): this.type = { state = state.clusterEnd(); this }

  def error(code: Int, message: String): this.type = { state = state.error(code, message); this }

  /**
   * Add arbitrary labrad data to this builder.
   *
   * This could fail if we are expecting data of a type that does not match.
   * TODO: check first if this is going to work to avoid partial completion?
   */
  def add(data: Data): this.type = {
    data.t match {
      case TNone => none()
      case TBool => bool(data.getBool)
      case TInt => int(data.getInt)
      case TUInt => uint(data.getUInt)
      case TTime => time(data.getSeconds, data.getFraction)
      case TStr => bytes(data.getBytes)
      case TValue(unitOpt) => value(data.getValue, unitOpt.getOrElse(""))
      case TComplex(unitOpt) => complex(data.getReal, data.getImag, unitOpt.getOrElse(""))
      case _: TArr =>
        array(data.arrayShape)
        for (elem <- data.flatIterator) {
          add(elem)
        }
      case _: TCluster =>
        clusterStart()
        for (elem <- data.clusterIterator) {
          add(elem)
        }
        clusterEnd()
      case _: TError =>
        error(data.getErrorCode, data.getErrorMessage)
        add(data.getErrorPayload)
    }
    this
  }

  /**
   * Add an object for which we have a ToData type class.
   *
   * The ToData takes this builder and the value as arguments and calls the
   * appropriate builder methods to add the value to this builder.
   * TODO: check first if this is going to work to avoid partial completion?
   */
  def add[T](value: T)(implicit toData: ToData[T]): this.type = {
    toData(this, value)
    this
  }

  /**
   * Get the resulting Data object. Requires that we are in the Done state.
   */
  def result(): Data = {
    state match {
      case Done(t) => new FlatData(t, buf.toByteArray, 0)
      case _ => throw new IllegalStateException(s"more data expected. state=$state")
    }
  }

  override def toString: String = {
    s"DataBuilder(state = $state, bytes = ${buf.readableBytes})"
  }

  /**
   * Narrow two types to a more specific type.
   *
   * This is needed to handle empty lists, which have indeterminate element
   * type, when embedded into composite structures. The type *_ for an empty
   * list can be narrowed to *s, for example, if we are later given strings.
   */
  def narrow(t1: Type, t2: Type): (Type, Boolean) = {
    (t1, t2) match {
      // empty array can be narrowed to any other array type
      case (TArr(TNone, d1), TArr(e2, d2)) if d1 == d2 => TArr(e2, d1) -> true
      case (TArr(e1, d1), TArr(TNone, d2)) if d1 == d2 => TArr(e1, d1) -> false

      // elements in composite types can be narrowed
      case (TArr(e1, d1), TArr(e2, d2)) if d1 == d2 =>
        val (e, narrowed) = narrow(e1, e2)
        TArr(e, d1) -> narrowed

      case (TCluster(elems1 @ _*), TCluster(elems2 @ _*)) if elems1.size == elems2.size =>
        val (elems, narrowings) = (elems1 zip elems2).map { case (e1, e2) => narrow(e1, e2) }.unzip
        TCluster(elems: _*) -> narrowings.exists(_ == true)

      case (TError(e1), TError(e2)) =>
        val (elem, narrowed) = narrow(e1, e2)
        TError(elem) -> narrowed

      // basic types must match, cannot be narrowed
      case (t1, t2) =>
        if (t1 != t2) throw new IllegalStateException(s"cannot narrow $t1 to $t2")
        t1 -> false
    }
  }

  /**
   * Determine whether a given type can be narrowed, that is, whether it is
   * or contains any empty list types.
   */
  def canNarrow(t: Type): Boolean = {
    t match {
      case TArr(TNone, d) => true
      case TCluster(elems @ _*) => elems.exists(canNarrow)
      case TError(payload) => canNarrow(payload)
      case _ => false
    }
  }

  /**
   * States keep track of what has been added to the builder so far and what we
   * expect next, so that we can ensure that we produce valid labrad data.
   *
   * States are arranged in a linked list like the activation frames in a call
   * stack, with the currently active state at the top and each state having a
   * reference to its caller. Each state can be thought of as a function (or,
   * more precisely, a coroutine) that consumes one chunk of data, possibly
   * calling into other states to consume parts of the data. When a given state
   * is done, it returns the type of data consumed (which may or may not be
   * known in advance) and resumes its caller.
   */
  sealed trait State {
    /**
     * The state that called us, to which we will return.
     */
    def caller: State

    /**
     * Call into this state. States are reusable, but not reentrant,
     * hence this may be called multiple time, but not concurrently.
     */
    def call(): State

    /**
     * Resume this state from another state that we called. This should
     * not be called explicitly; instead use ret() to return to the caller.
     */
    def resume(t: Type): State

    /**
     * Return the given type to our calling state.
     */
    def ret(t: Type): State = caller.resume(t)

    // These are the methods that can be invoked to advance the builder
    // and update the state. By default they all raise IllegalStateException,
    // but subclasses of State may override the methods that represent valid
    // continuations from that particular state.
    def none(): State = fail("add none")
    def bool(x: Boolean): State = fail("add bool")
    def int(x: Int): State = fail("add int")
    def uint(x: Long): State = fail("add uint")
    def bytes(x: Array[Byte]): State = fail("add bytes")
    def string(s: String): State = fail("add string")
    def time(seconds: Long, fraction: Long): State = fail("add time")
    def value(x: Double, unit: String = ""): State = fail("add value")
    def complex(re: Double, im: Double, unit: String = ""): State = fail("add complex")
    def array(shape: Array[Int]): State = fail("add array")
    def clusterStart(): State = fail("start cluster")
    def clusterEnd(): State = fail("end cluster")
    def error(code: Int, message: String): State = fail("add error")

    private def fail(msg: String) = {
      throw new IllegalStateException(s"cannot $msg in state $this")
    }

    /**
     * Create a human-readable representation of this state.
     *
     * Ideally, this would be drawn "top-down" but the caller linkages point
     * in the opposite direction. Hence, instead of rendering recursively, we
     * traverse the linked list of states and call the `render` method on each
     * one, passing in the rendered substate from its callee. State subclasses
     * must implement `render`.
     */
    override def toString: String = {
      var state = this
      var s = render("")
      while (state.caller != state) {
        state = state.caller
        s = state.render(s)
      }
      s
    }

    /**
     * Create a human-readable representation of this state, using the given
     * rendered subState.
     */
    def render(subState: String): String
  }

  /**
   * The top-level state. We call a consumer for the specified type if given
   * or an unknown type if not given, and then move to Done when it returns.
   */
  case class Start(tOpt: Option[Type]) extends State {
    def caller: State = this
    def call(): State = tOpt match {
      case None => ConsumeAny(caller = this).call()
      case Some(t) => consume(t, caller = this).call()
    }
    def resume(t: Type): State = {
      for (tExpected <- tOpt) {
        require(t == tExpected, s"expected $tExpected but builder produced $t")
      }
      Done(t)
    }
    override def render(subState: String): String = subState
  }

  /**
   * Final state when we are ready to build the result.
   */
  case class Done(t: Type) extends State {
    def caller: State = this
    def call(): State = ???
    def resume(t: Type): State = ???
    override def render(subState: String): String = s"Done($t)"
  }

  private def consume(t: Type, caller: State): State = {
    t match {
      case TArr(elem, depth) => ConsumeArray(elem, depth, caller)
      case TCluster(elems @ _*) => ConsumeCluster(elems, caller)
      case TError(payload) => ConsumeError(payload, caller)
      case t => ConsumeOne(t, caller)
    }
  }

  trait ConsumeSimple extends State {
    protected def check(t: Type): Unit

    override def none(): State = {
      check(TNone)
      ret(TNone)
    }
    override def bool(x: Boolean): State = {
      check(TBool)
      buf.writeBoolean(x)
      ret(TBool)
    }
    override def int(x: Int): State = {
      check(TInt)
      buf.writeInt(x)
      ret(TInt)
    }
    override def uint(x: Long): State = {
      check(TUInt)
      buf.writeInt(x.toInt)
      ret(TUInt)
    }
    override def bytes(x: Array[Byte]): State = {
      check(TStr)
      buf.writeLen { buf.writeBytes(x) }
      ret(TStr)
    }
    override def string(s: String): State = {
      check(TStr)
      buf.writeLen { buf.writeUtf8String(s) }
      ret(TStr)
    }
    override def time(seconds: Long, fraction: Long): State = {
      check(TTime)
      buf.writeLong(seconds)
      buf.writeLong(fraction)
      ret(TTime)
    }
    override def value(x: Double, unit: String = ""): State = {
      val t = TValue(Some(unit))
      check(t)
      buf.writeDouble(x)
      ret(t)
    }
    override def complex(re: Double, im: Double, unit: String = ""): State = {
      val t = TComplex(Some(unit))
      check(t)
      buf.writeDouble(re)
      buf.writeDouble(im)
      ret(t)
    }
  }

  case class ConsumeAny(caller: State) extends ConsumeSimple {
    def call(): State = this
    def resume(t: Type): State = ???

    // accept any type
    override protected def check(t: Type): Unit = {}

    override def array(shape: Array[Int]): State = ConsumeArrayAny(shape, caller = caller).call()
    override def clusterStart(): State = ConsumeClusterAny(caller = caller).call()
    override def clusterEnd(): State = caller.clusterEnd()
    override def error(code: Int, message: String): State = ConsumeErrorPayload(code, message, caller = caller).call()

    override def render(subState: String): String = "<?>"
  }

  case class ConsumeOne(t: Type, caller: State) extends ConsumeSimple {
    def call(): State = this
    def resume(t: Type): State = ???

    // accept only matching types
    override protected def check(t: Type): Unit = {
      require(t == this.t, s"cannot add $t. expecting ${this.t}")
    }

    override def render(subState: String): String = s"<$t>"
  }

  case class ConsumeClusterAny(caller: State) extends State {
    private val consumer = ConsumeAny(caller = this)
    private var i = 0
    private var elems: mutable.Buffer[Type] = _

    def call(): State = {
      i = 0
      elems = mutable.Buffer.empty[Type]
      consumer.call()
    }

    def resume(t: Type): State = {
      i += 1
      elems += t
      consumer.call()
    }

    override def clusterEnd(): State = {
      ret(TCluster(elems: _*))
    }

    override def render(subState: String): String = {
      val elemStrs = elems.map(_.toString).mkString
      s"($elemStrs$subState...)"
    }
  }

  case class ConsumeCluster(elems: Seq[Type], caller: State) extends State {
    private val size = elems.length
    private val types = elems.toArray
    private val consumers = elems.toArray.map(t => consume(t, caller = this))
    private val narrowable = elems.toArray.map(canNarrow)
    private var i = 0

    def call(): State = {
      i = 0
      this
    }

    def resume(t: Type): State = {
      if (narrowable(i)) {
        val (tt, narrowed) = narrow(types(i), t)
        if (narrowed) {
          types(i) = tt
          consumers(i) = consume(tt, caller = this)
        }
      }
      i += 1
      if (i < size) {
        consumers(i).call()
      } else {
        this
      }
    }

    override def clusterStart(): State = {
      require(i == 0, s"cannot start cluster in state $this")
      consumers(i).call()
    }

    override def clusterEnd(): State = {
      require(i == size, s"cannot end cluster. still expecting ${size - i} elements")
      ret(TCluster(types: _*))
    }

    override def render(subState: String): String = {
      val elemStrs = for ((elem, idx) <- types.zipWithIndex) yield {
        if (idx == i) {
          subState
        } else {
          elem.toString
        }
      }
      s"(${elemStrs.mkString})"
    }
  }

  case class ConsumeArrayAny(shape: Array[Int], caller: State) extends State {

    require(shape.length >= 1, s"expecting shape of length at least 1.")
    require(shape.forall(_ >= 0), s"dimensions must be nonnegative, got ${shape.mkString(",")}")

    private val depth = shape.length
    private val size: Int = shape.product

    private var i: Int = 0
    private var elem: Type = null
    private var narrowable = false
    private val anyConsumer = ConsumeAny(caller = this) // for first element
    private var consumer: State = null // for later elements when type is known

    def call(): State = {
      for (dim <- shape) {
        buf.writeInt(dim)
      }
      if (size == 0) {
        ret(TArr(TNone, depth))
      } else {
        i = 0
        anyConsumer.call()
      }
    }

    def resume(t: Type): State = {
      if (i == 0) {
        require(t != TNone)
        elem = t
        narrowable = canNarrow(t)
        consumer = consume(elem, caller = this)
      } else if (narrowable) {
        val (tt, narrowed) = narrow(elem, t)
        if (narrowed) {
          elem = tt
          narrowable = canNarrow(t)
          consumer = consume(elem, caller = this)
        }
      }
      i += 1
      if (i < size) {
        consumer.call()
      } else {
        ret(TArr(elem, depth))
      }
    }

    override def render(subState: String): String = {
      val dStr = if (depth == 1) "" else depth.toString
      s"*$dStr{$i/$size}$subState"
    }
  }

  case class ConsumeArray(t: Type, depth: Int, caller: State) extends State {
    private var elem: Type = t
    private var narrowable = canNarrow(elem)
    private var consumer = if (elem == TNone) {
      ConsumeAny(caller = this)
    } else {
      consume(elem, caller = this)
    }

    private var shape: Array[Int] = null
    private var size: Int = 0
    private var i: Int = 0

    def call(): State = {
      shape = null
      i = 0
      this
    }

    def resume(t: Type): State = {
      if (elem == TNone) {
        require(t != TNone)
        elem = t
        narrowable = canNarrow(elem)
        consumer = consume(elem, caller = this)
      } else if (narrowable) {
        val (tt, narrowed) = narrow(elem, t)
        if (narrowed) {
          elem = tt
          narrowable = canNarrow(elem)
          consumer = consume(elem, caller = this)
        }
      }
      i += 1
      if (i < size) {
        consumer.call()
      } else {
        ret(TArr(elem, depth))
      }
    }

    override def array(shape: Array[Int]): State = {
      require(shape.length == depth, s"expecting shape of depth $depth, got ${shape.length}")
      require(shape.forall(_ >= 0), s"dimensions must be nonnegative, got ${shape.mkString(",")}")
      this.shape = shape
      this.size = shape.product
      for (dim <- shape) {
        buf.writeInt(dim)
      }
      if (size == 0) {
        ret(TArr(elem, depth))
      } else {
        consumer.call()
      }
    }

    override def render(subState: String): String = {
      val dStr = if (depth == 1) "" else depth.toString
      if (shape == null) {
        s"*$dStr<shape>$elem"
      } else {
        s"*$dStr{$i/$size}$subState"
      }
    }
  }

  case class ConsumeError(payload: Type, caller: State) extends State {
    private val consumer = ConsumeOne(payload, caller = this)
    private var awaitingError = true

    def call(): State = {
      awaitingError = true
      this
    }

    def resume(t: Type): State = {
      val (tt, narrowed) = narrow(payload, t)
      ret(TError(tt))
    }

    override def error(code: Int, message: String): State = {
      buf.writeInt(code)
      buf.writeLen { buf.writeUtf8String(message) }
      awaitingError = false
      consumer.call()
    }

    override def render(subState: String): String = {
      if (awaitingError) {
        s"<E>$payload"
      } else {
        s"E$subState"
      }
    }
  }

  case class ConsumeErrorPayload(code: Int, message: String, caller: State) extends State {
    private val consumer = ConsumeAny(caller = this)
    private var i = 0

    def call(): State = {
      buf.writeInt(code)
      buf.writeLen { buf.writeUtf8String(message) }
      consumer.call()
    }

    def resume(t: Type): State = {
      ret(TError(t))
    }

    override def render(subState: String): String = {
      s"E$subState"
    }
  }
}
