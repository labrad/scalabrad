//package org.labrad.util.exp1
//
//import org.labrad.util.concurrent.{Broker, Offer}
//import scala.collection.mutable
//import scala.concurrent.{Await, ExecutionContext => EC, Future, Lock, Promise, SyncVar}
//import scala.concurrent.duration._
//import scala.util.continuations._
//
//import scala.language.experimental.macros
//
//object Go {
//
//  // TODO: create our own execution context that is only for goroutines, rather than using EC.global
//  private val ec = EC.global
//
//  def apply[A](f: => A @cpsParam[A, Any]): Future[A] = {
//    val p = Promise[A]
//    schedule(new Runnable {
//      def run = {
//        reify(f).foreachFull(a => p.success(a), ex => p.failure(ex))
//      }
//    })
//    p.future
//  }
//
//  def main(f: => Unit @suspendable): Unit = {
//    Await.result(apply(f), Duration.Inf)
//  }
//
//  def long[A](f: => A @cpsParam[A, Any]): Future[A] = {
//    apply(f) // TODO: run this on a separate thread pool so as not to block event threads
//  }
//
//  private[util] def schedule(r: Runnable): Unit = ec.execute(r)
//}
//
//trait Send[A] {
//  def ! (a: A): Unit @suspendable
//}
//
//trait Recv[A] {
//  def ? : A @suspendable
//}
//
//class Chan[A] extends Send[A] with Recv[A] {
//
//  private var items = mutable.Queue.empty[(A, Unit => Unit)]
//  private var receivers = mutable.Queue.empty[A => Unit]
//
//  def ! (a: A): Unit @suspendable =
//    shift { s: (Unit => Unit) =>
//      synchronized {
//        if (receivers.isEmpty) {
//          println("no receivers; queueing item")
//          items += (a -> s)
//        } else {
//          println("have receivers; completing send")
//          val r = receivers.dequeue
//          Go.schedule(new Runnable { def run = r(a) })
//          Go.schedule(new Runnable { def run = s() })
//        }
//        ()
//      }
//  }
//
//  def ? : A @suspendable =
//    shift { r: (A => Unit) =>
//      synchronized {
//        if (items.isEmpty) {
//          println("no items; queueing receiver")
//          receivers += r
//        } else {
//          println("have items; completing receive")
//          val (a, s) = items.dequeue
//          Go.schedule(new Runnable { def run = r(a) })
//          Go.schedule(new Runnable { def run = s() })
//        }
//        ()
//      }
//    }
//}
//
//object Chan {
//  def apply[A] = new Chan[A]
//}
//
//object GoMain {
//  def main(args: Array[String]): Unit = {
////    val intChan = new Chan[Int]
////    val strChan = new Chan[String]
////    val getSums = new Chan[Unit]
////    val replyChan = new Chan[(Int, String)]
////
////    Go {
////      var ints = Seq.empty[Int]
////      var strs = Seq.empty[String]
////
////      val a = intChan.?
////      val b = intChan.?
////      val c = intChan.?
////      getSums.?
////      replyChan ! ((a + b + c, "abc"))
////    }
////
////    Go.main {
////      intChan ! 1
////      intChan ! 2
////      intChan ! 3
////      getSums ! (())
////
////      val (n, s) = replyChan.?
////      println(s"sums: $n, $s")
////    }
//  }
//}
