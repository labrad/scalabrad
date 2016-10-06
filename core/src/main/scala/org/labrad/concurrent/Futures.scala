package org.labrad.concurrent

import org.labrad.util.Await
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Helpers for dealing with scala Futures in a blocking fashion.
 */
object Futures {

  /**
   * Add a deadline-aware await() method to Futures. Waits for the result of
   * the future up to the given deadline or until the timeout elapses, whichever
   * comes first. By default, the wait time is 2^63-1 nanoseconds, which
   * is the longest finite time expressible as a Duration (about 292 years).
   */
  implicit class AwaitableFuture[A](val f: Future[A]) extends AnyVal {
    def await(timeout: Duration = Duration.Inf)(implicit deadline: Deadline): A = {
      val remaining = (timeout, deadline) match {
        case (t, null) if t.isFinite => timeout.toNanos
        case (t, null)               => Long.MaxValue
        case (t, d) if t.isFinite    => t.toNanos min d.timeLeft.toNanos
        case (t, d)                  => d.timeLeft.toNanos
      }
      Await.result(f, remaining.nanoseconds)
    }
  }

}
