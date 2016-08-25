package org.labrad.concurrent

import org.labrad.concurrent.Go._
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.Duration
import scala.util.{Try, Success, Failure}

object Coro {
  def apply[In, Out](body: (In, Out => In) => Out): Coro[In, Out] = {
    new Coro(body)
  }
}

class Coro[In, Out](body: (In, Out => In) => Out) {

  private val chan = Chan[(Try[In], Promise[Out])](1)

  private lazy val f = go {
    var promise: Promise[Out] = null
    val next = (out: Out) => {
      promise.trySuccess(out)
      val (in, p) = chan.recv()
      promise = p
      in.get
    }
    val (in, p) = chan.recv()
    promise = p
    body(in.get, next)
  }

  def sendFuture(in: In): Future[Out] = {
    var promise = Promise[Out]
    promise.tryCompleteWith(f)
    chan.send((Success(in), promise))
    promise.future
  }

  def send(in: In)(implicit timeout: Duration): Out = {
    Await.result(sendFuture(in), timeout)
  }

  def raiseFuture(throwable: Throwable): Future[Out] = {
    var promise = Promise[Out]
    promise.tryCompleteWith(f)
    chan.send((Failure(throwable), promise))
    promise.future
  }

  def raise(throwable: Throwable)(implicit timeout: Duration): Out = {
    Await.result(raiseFuture(throwable), timeout)
  }

  def close(): Unit = {
    raiseFuture(new InterruptedException)
  }
}
