package org.labrad.util.concurrent

import akka.actor.Scheduler
import scala.concurrent.{Future, Promise, Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.Random

/**
 * An offer to communicate with another process. The offer is
 * parameterized on the type of the value communicated. An offer that
 * sends a value typically has type {{Unit}}. An offer is activated by
 * synchronizing it, which is done with `sync()`.
 *
 * Note that Offers are persistent values -- they may be synchronized
 * multiple times. They represent a standing offer of communication, not
 * a one-shot event.
 *
 * =The synchronization protocol=
 *
 * Synchronization is performed via a two-phase commit process.
 * `prepare()` commenses the transaction, and when the other party is
 * ready, it returns with a transaction object, `Tx[T]`. This must then
 * be ackd or nackd. If both parties acknowledge, `Tx.ack()` returns
 * with a commit object, containing the value. This finalizes the
 * transaction. Please see the `Tx` documentation for more details on
 * that phase of the protocol.
 *
 * Note that a user should never perform this protocol themselves --
 * synchronization should always be done with `sync()`.
 *
 * Future interrupts are propagated, and failure is passed through. It
 * is up to the implementor of the Offer to decide on failure semantics,
 * but they are always passed through in all of the combinators.
 */
trait Offer[T] { self =>

  /**
   * Prepare a new transaction. This is the first stage of the 2 phase
   * commit. This is typically only called by the offer implementation
   * directly or by combinators.
   */
  private[concurrent] def prepare(): Promise[Tx[T]]

  /**
   * Synchronizes this offer, returning a future representing the result
   * of the synchronization.
   */
  def sync()(implicit ec: ExecutionContext): Future[T] =
    prepare().future flatMap { tx =>
      tx.ack() flatMap {
        case Tx.Commit(v) => Future.successful(v)
        case Tx.Abort => sync()
      }
    }

  /**
   * Synonym for `sync()`
   */
  @deprecated("use sync() instead", "5.x")
  def apply()(implicit ec: ExecutionContext): Future[T] = sync()

  /* Combinators */

  /**
   * Map this offer of type {{T}} into one of type {{U}}.  The
   * translation (performed by {{f}}) is done after the {{Offer[T]}} has
   * successfully synchronized.
   */
  def map[U](f: T => U)(implicit ec: ExecutionContext): Offer[U] = new Offer[U] {
    def prepare() = Promise[Tx[U]].tryCompleteWith {
      self.prepare().future map { tx =>
        new Tx[U] {
          import Tx.{Commit, Abort}
          def ack() = tx.ack() map {
            case Commit(t) => Commit(f(t))
            case Abort => Abort
          }

          def nack() { tx.nack() }
        }
      }
    }
  }

  /**
   * Synonym for `map()`. Useful in combination with `Offer.choose()`
   * and `Offer.select()`
   */
  def apply[U](f: T => U)(implicit ec: ExecutionContext): Offer[U] = map(f)

  /**
   * Like {{map}}, but to a constant (call-by-name).
   */
  def const[U](f: => U)(implicit ec: ExecutionContext): Offer[U] = map { _ => f }

  /**
   * An offer that, when synchronized, attempts to synchronize {{this}}
   * immediately, and if it fails, synchronizes on {{other}} instead.  This is useful
   * for providing default values. Eg.:
   *
   * {{{
   * offer orElse Offer.const { computeDefaultValue() }
   * }}}
   */
  def orElse[U >: T](other: Offer[U])(implicit ec: ExecutionContext): Offer[U] = new Offer[U] {
    def prepare() = {
      val ourTx = self.prepare()
      if (ourTx.isCompleted) ourTx.asInstanceOf[Promise[Tx[U]]] else {
        ourTx.future.onSuccess { case tx => tx.nack() }
        other.prepare()
      }
    }
  }

  def or[U](other: Offer[U])(implicit ec: ExecutionContext): Offer[Either[T, U]] =
    Offer.choose(this map { Left(_) }, other map { Right(_) })

  /**
   * Synchronize on this offer indefinitely, invoking the given {{f}}
   * with each successfully synchronized value.  A receiver can use
   * this to enumerate over all received values.
   */
  def foreach(f: T => Unit)(implicit ec: ExecutionContext) {
    sync() foreach { v =>
      f(v)
      foreach(f)
    }
  }

  /**
   * Synchronize (discarding the value), and then invoke the given
   * closure.  Convenient for loops.
   */
  def andThen(f: => Unit)(implicit ec: ExecutionContext) {
    sync() onSuccess { case _ => f }
  }

  /**
   * Synchronize this offer, blocking for the result. See {{sync()}}
   * and {{com.twitter.util.Future.apply()}}
   */
  def syncWait()(implicit ec: ExecutionContext): T = Await.result(sync(), Duration.Inf) // TODO: timeout?

  /* Scala actor-style syntax */

  /**
   * Alias for synchronize.
   */
  def ? (implicit ec: ExecutionContext) = sync()

  /**
   * Synchronize, blocking for the result.
   */
  def ?? (implicit ec: ExecutionContext) = syncWait()
}

object Offer {
  /**
   * A constant offer: synchronizes the given value always. This is
   * call-by-name and a new value is produced for each `prepare()`.
   */
  def const[T](x: => T): Offer[T] = new Offer[T] {
    def prepare() = Promise.successful(Tx.const(x))
  }

  /**
   * An offer that never synchronizes.
   */
  def never[T]: Offer[T] = new Offer[T] {
    def prepare() = Promise[Tx[T]]
  }

  /**
   * The offer that chooses exactly one of the given offers. If there are any
   * Offers that are synchronizable immediately, one is chosen at random.
   */
  def choose[T](evs: Offer[T]*)(implicit ec: ExecutionContext): Offer[T] = if (evs.isEmpty) Offer.never[T] else new Offer[T] {
    def prepare(): Promise[Tx[T]] = {
      val rng = new Random(System.nanoTime)
      val prepd = rng.shuffle(evs map { _.prepare() })

      prepd find(_.isCompleted) match {
        case Some(winner) =>
          for (loser <- prepd if loser ne winner) {
            loser.future.onSuccess { case tx => tx.nack() }
          }
          winner

        case None =>
          val futures = prepd.map(p => p.future.map { tx => (p, tx) })
          Promise[Tx[T]].tryCompleteWith {
            Future.firstCompletedOf(futures) map { case (winner, tx) =>
              for (loser <- prepd if loser ne winner) {
                loser.future.onSuccess { case tx => tx.nack() }
              }
              tx
            }
          }
      }
    }
  }

  /**
   * `Offer.choose()` and synchronize it.
   */
  def select[T](ofs: Offer[T]*)(implicit ec: ExecutionContext): Future[T] = choose(ofs: _*).sync()

  /**
   * An offer that is available after the given time out.
   */
  def timeout(timeout: Duration)(implicit ec: ExecutionContext, scheduler: Scheduler): Offer[Unit] = new Offer[Unit] {
    private[this] val tx = Tx.const(())

    def prepare() = {
      val p = Promise[Tx[Unit]]
      if (timeout.isFinite) {
        scheduler.scheduleOnce(timeout.asInstanceOf[FiniteDuration]) { p.trySuccess(tx) }
      }
      p
    }
  }
}
