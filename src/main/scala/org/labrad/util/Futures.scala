package org.labrad.util

import io.netty.util.concurrent.{Future => NettyFuture, GenericFutureListener}
import java.util.concurrent.{ScheduledExecutorService, TimeoutException, TimeUnit}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._

object Futures {

  /**
   * Add a .toScala method to netty futures to convert them to scala futures.
   */
  implicit class RichNettyFuture[A](val future: NettyFuture[A]) extends AnyVal {
    def toScala: Future[A] = {
      val p = Promise[A]
      future.addListener(new GenericFutureListener[NettyFuture[A]] {
        def operationComplete(f: NettyFuture[A]) {
          f.isSuccess match {
            case true => p.success(f.getNow)
            case false => p.failure(f.cause)
          }
        }
      })
      p.future
    }
  }

  implicit class RichFuture[A](val future: Future[A]) extends AnyVal {
    def withTimeout(timeout: Duration)(implicit executor: ExecutionContext, scheduler: ScheduledExecutorService): Future[A] = {
      val p = Promise[A]
      val timeoutTask = scheduler.schedule(new Runnable {
        def run: Unit = {
          p.tryFailure(new TimeoutException())
        }
      }, timeout.toNanos, TimeUnit.NANOSECONDS)
      future.onComplete { result =>
        timeoutTask.cancel(false)
        p.tryComplete(result)
      }
      p.future
    }
  }
}
