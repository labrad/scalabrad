package org.labrad.util

import java.util.concurrent.{Executors, TimeoutException}
import org.labrad.util.Futures._
import org.scalatest._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._

class FuturesTest extends FunSuite {

  implicit val executionContext = ExecutionContext.global
  implicit val timeoutScheduler = Executors.newSingleThreadScheduledExecutor()

  test("future.withTimeout returns normally if future completes first") {
    val f = Future.successful("ok").withTimeout(100.millis)
    val result = Await.result(f, 200.millis)
    assert(result == "ok")
  }

  test("future.withTimeout raises TimeoutException if timeout elapses") {
    val p = Promise[String]
    val f = p.future.withTimeout(10.millis)
    intercept[TimeoutException] {
      Await.result(f, 2000.millis)
    }
  }

  class TestException extends Exception with Rethrowable {
    def copy: TestException = new TestException
  }

  test("future.withTimeout raises error from failed future") {
    val f = Future.failed(new TestException).withTimeout(100.millis)
    intercept[TestException] {
      Await.result(f, 200.millis)
    }
  }
}
