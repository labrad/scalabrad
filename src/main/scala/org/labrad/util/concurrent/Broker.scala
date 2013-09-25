package org.labrad.util.concurrent

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.{Future, Promise, Await, ExecutionContext}
import scala.concurrent.duration._

/**
 * An unbuffered FIFO queue, brokered by `Offer`s. Note that the queue is
 * ordered by successful operations, not initiations, so `one` and `two`
 * may not be received in that order with this code:
 *
 * {{{
 * val b: Broker[Int]
 * b ! 1
 * b ! 2
 * }}}
 *
 * But rather we need to explicitly sequence them:
 *
 * {{{
 * val b: Broker[Int]
 * for {
 *   () <- b ! 1
 *   () <- b ! 2
 * } ()
 * }}}
 *
 * BUGS: the implementation would be much simpler in the absence of
 * cancellation.
 */

class Broker[T] {
  private[this] sealed trait State
  private[this] case object Quiet extends State
  private[this] case class Sending(q: Queue[(Promise[Tx[Unit]], T)]) extends State
  private[this] case class Receiving(q: Queue[Promise[Tx[T]]]) extends State

  private[this] val state = new AtomicReference[State](Quiet)

  def send(msg: T): Offer[Unit] = new Offer[Unit] {
    @tailrec
    def prepare() = {
      state.get match {
        case s @ Receiving(rq) =>
          if (rq.isEmpty) throw new IllegalStateException()
          val (recvp, newq) = rq.dequeue
          val nextState = if (newq.isEmpty) Quiet else Receiving(newq)
          if (!state.compareAndSet(s, nextState)) prepare() else {
            val (sendTx, recvTx) = Tx.twoParty(msg)
            recvp.trySuccess(recvTx)
            Promise.successful(sendTx)
          }

        case s @ (Quiet | Sending(_)) =>
          val p = Promise[Tx[Unit]]
          val elem: (Promise[Tx[Unit]], T) = (p, msg)
          val nextState = s match {
            case Quiet => Sending(Queue(elem))
            case Sending(q) => Sending(q enqueue elem)
            case Receiving(_) => throw new IllegalStateException()
          }

          if (!state.compareAndSet(s, nextState)) prepare() else p
      }
    }
  }

  val recv: Offer[T] = new Offer[T] {
    @tailrec
    def prepare() =
      state.get match {
        case s @ Sending(sq) =>
          if (sq.isEmpty) throw new IllegalStateException()
          val ((sendp, msg), newq) = sq.dequeue
          val nextState = if (newq.isEmpty) Quiet else Sending(newq)
          if (!state.compareAndSet(s, nextState)) prepare() else {
            val (sendTx, recvTx) = Tx.twoParty(msg)
            sendp.trySuccess(sendTx)
            Promise.successful(recvTx)
          }

        case s @ (Quiet | Receiving(_)) =>
          val p = Promise[Tx[T]]
          val nextState = s match {
            case Quiet => Receiving(Queue(p))
            case Receiving(q) => Receiving(q enqueue p)
            case Sending(_) => throw new IllegalStateException()
          }

          if (!state.compareAndSet(s, nextState)) prepare() else p
      }
  }

  /* Scala actor style / CSP syntax. */

  /**
   * Send an item on the broker, returning a {{Future}} indicating
   * completion.
   */
  def !(msg: T)(implicit ec: ExecutionContext): Future[Unit] = send(msg).sync()

  /**
   * Like {!}, but block until the item has been sent.
   */
  def !!(msg: T)(implicit ec: ExecutionContext): Unit = Await.result(this ! msg, Duration.Inf)

  /**
   * Retrieve an item from the broker, asynchronously.
   */
  def ?(implicit ec: ExecutionContext): Future[T] = recv.sync()

  /**
   * Retrieve an item from the broker, blocking.
   */
  def ??(implicit ec: ExecutionContext): T = Await.result(this.?, Duration.Inf)
}

class BufferedBroker[T](capacity: Int) {
  private[this] sealed trait State
  private[this] case object Quiet extends State
  private[this] case class Sending(q: Queue[(Promise[Tx[Unit]], Option[Tx[T]], T)]) extends State
  private[this] case class Receiving(q: Queue[Promise[Tx[T]]]) extends State

  private[this] val state = new AtomicReference[State](Quiet)

  def send(msg: T): Offer[Unit] = new Offer[Unit] {
    @tailrec
    def prepare() = {
      state.get match {
        case s @ Receiving(rq) =>
          if (rq.isEmpty) throw new IllegalStateException()
          val (recvp, newq) = rq.dequeue
          val nextState = if (newq.isEmpty) Quiet else Receiving(newq)
          if (!state.compareAndSet(s, nextState)) prepare() else {
            val (sendTx, recvTx) = Tx.twoParty(msg)
            recvp.trySuccess(recvTx)
            Promise.successful(sendTx)
          }

        case s @ (Quiet | Sending(_)) =>
          // if buffer is not full, send immediately
          val buffered = s match {
            case Quiet => 0
            case Sending(q) => q.count(_._2.isDefined)
            case Receiving(_) => throw new IllegalStateException()
          }
          val (p, recvTxOpt) = if (buffered < capacity) {
            val (sendTx, recvTx) = Tx.twoParty(msg)
            (Promise.successful[Tx[Unit]](sendTx), Some(recvTx))
          } else {
            (Promise[Tx[Unit]], None)
          }

          val elem: (Promise[Tx[Unit]], Option[Tx[T]], T) = (p, recvTxOpt, msg)
          val nextState = s match {
            case Quiet => Sending(Queue(elem))
            case Sending(q) => Sending(q enqueue elem)
            case Receiving(_) => throw new IllegalStateException()
          }
          if (!state.compareAndSet(s, nextState)) prepare() else p
      }
    }
  }

  val recv: Offer[T] = new Offer[T] {
    @tailrec
    def prepare() = {
      state.get match {
        case s @ Sending(sq) =>
          if (sq.isEmpty) throw new IllegalStateException()
          val ((sendp, recvTxOpt, msg), newq) = sq.dequeue
          val nextState = if (newq.isEmpty) Quiet else Sending(newq)
          if (!state.compareAndSet(s, nextState)) prepare() else {
            val recvTx = recvTxOpt match {
              case Some(recvTx) => recvTx // the transaction was already created
              case None =>
                val (sendTx, recvTx) = Tx.twoParty(msg)
                sendp.trySuccess(sendTx)
                recvTx
            }
            Promise.successful(recvTx)
          }

        case s @ (Quiet | Receiving(_)) =>
          val p = Promise[Tx[T]]
          val nextState = s match {
            case Quiet => Receiving(Queue(p))
            case Receiving(q) => Receiving(q enqueue p)
            case Sending(_) => throw new IllegalStateException()
          }
          if (!state.compareAndSet(s, nextState)) prepare() else p
      }
    }
  }

  /* Scala actor style / CSP syntax. */

  /**
   * Send an item on the broker, returning a {{Future}} indicating
   * completion.
   */
  def !(msg: T)(implicit ec: ExecutionContext): Future[Unit] = send(msg).sync()

  /**
   * Like {!}, but block until the item has been sent.
   */
  def !!(msg: T)(implicit ec: ExecutionContext): Unit = Await.result(this ! msg, Duration.Inf)

  /**
   * Retrieve an item from the broker, asynchronously.
   */
  def ?(implicit ec: ExecutionContext): Future[T] = recv.sync()

  /**
   * Retrieve an item from the broker, blocking.
   */
  def ??(implicit ec: ExecutionContext): T = Await.result(this.?, Duration.Inf)
}
