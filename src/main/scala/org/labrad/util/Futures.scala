package org.labrad.util

import io.netty.util.concurrent.{Future => NettyFuture, GenericFutureListener}
import scala.concurrent.{Future, Promise}

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
}
