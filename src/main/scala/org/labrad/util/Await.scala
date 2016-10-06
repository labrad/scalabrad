package org.labrad.util

import java.util.concurrent.TimeoutException
import scala.concurrent.Future
import scala.concurrent.duration._

object Await {
  /**
   * Convenience method to await a Future with a default timeout, which
   * can be overridden if desired
   */
  def apply[A](f: Future[A], timeout: Duration = 30.seconds): A = {
    result(f, timeout)
  }

  /**
   * Await the result of a Future, or raise a TimeoutException if the result
   * does not arrive within the given timeout. This has the same interface as
   * scala.concurrent.Await except that if the Future fails we throw a new
   * exception created here. This ensures that the line where Await was called
   * appears in the stack trace, as opposed to scala.concurrent.Await which
   * just rethrows the original exception including its stack trace and so the
   * exception which propagates does not include the stack up to the point
   * where .result was called.
   *
   * Because we want to throw a new exception to get stack trace information,
   * we have to wrap the thrown exception with our own exception, which means
   * we lose the exception type. To deal with this, custom exception types can
   * extend Rethrowable which provides machinery for creating a copy of the
   * exception which will capture the local stack trace but refer back to the
   * original exception as the underlying cause.
   */
  def result[A](f: Future[A], timeout: Duration): A = {
    try {
      scala.concurrent.Await.result(f, timeout)
    } catch {
      case cause: Rethrowable =>
        cause.rethrow()

      case cause: TimeoutException =>
        val e = new TimeoutException
        e.initCause(cause)
        throw e

      case cause: Exception =>
        throw new RuntimeException(cause)
    }
  }
}

/**
 * Superclass for exceptions that know how to copy themselves to be rethrown.
 * This is used by Await to throw the same Exception type but a new local stack
 * trace when dealing with failed Futures.
 */
trait Rethrowable extends Throwable {
  def rethrow(): Nothing = {
    val ex = copy()
    if (ex eq this) {
      throw this
    }
    ex.initCause(this)
    throw ex
  }

  def copy(): Throwable
}
