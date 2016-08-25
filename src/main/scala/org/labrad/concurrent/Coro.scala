package org.labrad.concurrent

import java.util.{Timer, TimerTask}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.{Lock, ReentrantLock}
import org.labrad.concurrent.Go._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration
import scala.util.{Try, Success, Failure}

trait Yield[In, Out] {
  def apply(out: Out): In
}

object Coro {
  def apply[In, Out](body: (In, Yield[In, Out]) => Out)(implicit ec: ExecutionContext): Coro[In, Out] = {
    new Coro(body)
  }
}

class Coro[In, Out](body: (In, Yield[In, Out]) => Out)(implicit ec: ExecutionContext) {

  private val chan = Chan[(Try[In], Promise[Out])](1)

  private lazy val f = go {
    var promise: Promise[Out] = null
    val yieldFunc = new Yield[In, Out] {
      def apply(out: Out): In = {
        promise.trySuccess(out)
        val (in, p) = chan.recv()
        promise = p
        in.get
      }
    }
    val (in, p) = chan.recv()
    promise = p
    body(in.get, yieldFunc)
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
}
