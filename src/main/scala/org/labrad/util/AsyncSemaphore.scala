package org.labrad.util

/**
 * An AsyncSemaphore is a traditional semaphore but with asynchronous
 * execution. Grabbing a permit returns a Future[Permit]
 *
 * modified from com.twitter.util.concurrent to use scala futures
 */

import java.util.concurrent.RejectedExecutionException
import scala.collection.mutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}

trait Permit {
  def release()
}

class AsyncSemaphore(initialPermits: Int = 0, maxWaiters: Option[Int] = None) { self =>
  require(maxWaiters.getOrElse(0) >= 0)
  @volatile private[this] var waiters = new Queue[() => Unit]
  @volatile private[this] var availablePermits = initialPermits

  private[this] class SemaphorePermit extends Permit {
    /**
     * Indicate that you are done with your Permit.
     */
    def release() = {
      val run = self.synchronized {
        availablePermits += 1
        if (availablePermits > 0 && !waiters.isEmpty) {
          availablePermits -= 1
          Some(waiters.dequeue())
        } else {
          None
        }
      }

      run foreach { _() }
    }
  }

  def numWaiters = waiters.size
  def numPermitsAvailable = availablePermits

  /**
   * Acquire a Permit, asynchronously. Be sure to permit.release() in a 'finally'
   * block of your onSuccess() callback.
   *
   * @return a Future[Permit] when the Future is satisfied, computation can proceed,
   * or a Future.Exception[RejectedExecutionException] if the configured maximum number of waiters
   * would be exceeded.
   */
  def acquire(): Future[Permit] = {
    val result = Promise[Permit]

    def setAcquired(): Unit = result.success(new SemaphorePermit)

    val (isException, runNow) = self.synchronized {
      if (availablePermits > 0) {
        availablePermits -= 1
        (false, true)
      } else {
        maxWaiters match {
          case Some(max) if (waiters.size >= max) =>
            (true, false)
          case _ =>
            waiters.enqueue(setAcquired)
            (false, false)
        }
      }
    }

    if (isException) {
      AsyncSemaphore.MaxWaitersExceededException
    } else {
      if (runNow) setAcquired()
      result.future
    }
  }

  def map[A](f: => A)(implicit context: ExecutionContext): Future[A] =
    acquire().map { permit =>
      try f finally permit.release()
    }

  def flatMap[A](f: => Future[A])(implicit context: ExecutionContext): Future[A] =
    acquire().flatMap { permit =>
      val result = f
      result.onComplete(_ => permit.release())
      result
    }
}

object AsyncSemaphore {
  private val MaxWaitersExceededException =
    Future.failed(new RejectedExecutionException("Max waiters exceeded"))
}