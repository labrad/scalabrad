package org.labrad.util

import java.util.concurrent.TimeoutException
import scala.concurrent.Future
import scala.concurrent.duration._

object Await {
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
