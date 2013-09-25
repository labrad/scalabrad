//package org.labrad.util
//
//import org.labrad.util.concurrent.{Broker, Offer}
//import scala.collection.mutable
//import scala.concurrent.{Await, ExecutionContext => EC, Future, Lock, Promise, SyncVar}
//import scala.concurrent.duration._
//import scala.util.continuations._
//import scala.util.{Failure, Success, Try}
//
//object Go {
//
//  // TODO: create our own execution context that is only for goroutines, rather than using EC.global
//  private val ec = EC.global
//
//  def apply[A](f: => A @suspendable): Future[A] = {
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
//  def long(f: => Unit @suspendable): Future[Unit] = {
//    apply(f) // TODO: run this on a separate thread pool so as not to block event threads
//  }
//
//  def await[A](f: Future[A]): A @suspendable = {
//    val result = shift { k: (Try[A] => Unit) =>
//      f.onComplete(k)(ec)
//    }
//    result match {
//      case Success(a) => a
//      case Failure(e) => throw e
//    }
//  }
//
//  private[util] def schedule(r: Runnable): Unit = ec.execute(r)
//
//  implicit class RichBroker[A](chan: Broker[A]) {
//    def <-- (a: A)(implicit ec: EC): Unit @suspendable = await { chan ! a }
//    def --> (implicit ec: EC): A @ suspendable = await { chan.? }
//  }
//}
//
//object GoMain {
//  def main(args: Array[String]): Unit = {
//    import scala.concurrent.ExecutionContext.Implicits.global
//    import Go._
//
//    val intChan = new Broker[Int]
//    val strChan = new Broker[String]
//    val getSums = new Broker[Unit]
//    val replyChan = new Broker[(Int, String)]
//
//    Go {
//      var ints = Seq.empty[Int]
//      var strs = Seq.empty[String]
//
//      Offer.choose(
//        intChan.recv { a => ints :+= a },
//        strChan.recv { s => strs :+= s },
//        getSums.recv { _ => replyChan ! ((ints.sum, strs.mkString)); () }
//      ).foreach { _ => println("got message!") }
//    }
//
//    val sendEarly = false
//
//    Go.main {
//      if (sendEarly) {
//        val f1 = { println("A"); intChan ! 1 }
//        val f2 = { println("B"); intChan ! 2 }
//        val f3 = { println("C"); intChan ! 3 }
//        val f4 = { println("D"); getSums ! (()) }
//        val f = for {
//          _ <- f1
//          _ <- f2
//          _ <- f3
//          _ <- f4
//          (n, s) <- { println("E"); replyChan.? }
//        } yield println(s"sums: $n, $s")
//        Await.result(f, 5.seconds)
//      } else {
//        var i = 1
//        while (i < 100) {
//          intChan <-- i
//          i += 1
//        }
//        strChan <-- "c"
//        strChan <-- "d"
//        getSums <-- (())
//        val (n, s) = replyChan.-->
//        println(s"sums: $n, $s")
//      }
//    }
//  }
//}
