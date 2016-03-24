package org.labrad.concurrent

import java.util.{Timer, TimerTask}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.{Lock, ReentrantLock}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.Try

// TODO:
// - close channels (two-item receive?)
// - iterate over channels
// - timers and tickers (stoppable)

object Go {

  private val threadGroup = new ThreadGroup("Go")

  private val threadFactory = new ThreadFactory {
    val counter = new AtomicLong(0)
    def newThread(r: Runnable): Thread = {
      val i = counter.getAndIncrement()
      val thread = new Thread(threadGroup, r, s"Go$i")
      thread.setDaemon(true)
      thread
    }
  }

  private[concurrent] val executor = Executors.newCachedThreadPool(threadFactory)
  private[concurrent] val timer = new Timer("GoTimer", true)

  private val executionContext = ExecutionContext.fromExecutorService(executor)

  def go[A](f: => A): Future[A] = {
    Future(f)(executionContext)
  }

  def select[A](options: Selection[A]*): A = {
    var (readies, default) = {
      val builder = Seq.newBuilder[Selection[A]]
      var default: Selection[A] = null
      for (option <- options) {
        option.lock()
        if (option.isReady) {
          if (option.isDefault) {
            default = option
          } else {
            builder += option
          }
        }
      }
      (builder.result, default)
    }
    val k = try {
      var kOpt: Option[() => A] = None
      if (!readies.isEmpty) {
        val rand = ThreadLocalRandom.current()
        while (readies.length > 0 && kOpt == None) {
          val idx = rand.nextInt(0, readies.length)
          val choice = readies(idx)
          kOpt = choice.select()
          if (kOpt == None) {
            readies = readies.patch(idx, Nil, 1)
          }
        }
      }
      if (!kOpt.isEmpty) {
        kOpt.get
      } else {
        if (default != null) {
          default.select().get
        } else {
          val cell = new Cell[() => A]
          for (opt <- options) {
            opt.register(cell)
          }
          () => {
            val k = cell.get
            k()
          }
        }
      }
    } finally {
      for (option <- options) {
        option.unlock()
      }
    }
    k()
  }

  def default[A](f: => A): Selection[A] = new Selection[A] {
    override def isDefault = true

    def lock(): Unit = {}
    def unlock(): Unit = {}

    def isReady: Boolean = true
    def select(): Option[() => A] = Some(() => f)
    def register[AA >: A](cell: Cell[() => AA]): Unit = throw new IllegalStateException
  }

  implicit class SelectableFuture[A](val future: Future[A])(implicit ec: ExecutionContext = executionContext) extends Recv[Try[A]] {
    def onRecv[B](f: Try[A] => B): Selection[B] = new Selection[B] {
      def lock(): Unit = {}
      def unlock(): Unit = {}

      def isReady: Boolean = future.isCompleted
      def select(): Option[() => B] = {
        if (isReady) {
          Some(() => f(future.value.get))
        } else {
          None
        }
      }
      def register[BB >: B](cell: Cell[() => BB]): Unit = {
        future.onComplete { result =>
          cell.put(() => f(result))
        }
      }
    }
  }
}

trait Selection[+A] {
  def isDefault: Boolean = false

  def lock(): Unit
  def unlock(): Unit

  def isReady: Boolean
  def select(): Option[() => A]
  def register[AA >: A](cell: Cell[() => AA]): Unit

  def sync: A = Go.select(this)
}

class Cell[A] {
  private val lock = new ReentrantLock
  private val cond = lock.newCondition()
  private var isSet: Boolean = false
  private var value: A = _

  def put(a: A): Boolean = {
    lock.lock()
    try {
      val success = !isSet
      if (success) {
        value = a
        isSet = true
        cond.signal()
      }
      success
    } finally {
      lock.unlock()
    }
  }

  def get: A = {
    lock.lock()
    try {
      while (!isSet) {
        cond.await()
      }
      value
    } finally {
      lock.unlock()
    }
  }
}

trait Recv[A] {
  def onRecv[B](f: A => B): Selection[B]
  def -->[B](f: A => B): Selection[B] = onRecv(f)

  def recv(): A = onRecv(a => a).sync
}

trait Send[A] {
  def onSend[B](a: A)(f: => B): Selection[B]
  def <--[B](a: A)(f: => B): Selection[B] = onSend(a)(f)

  def send(a: A): Unit = onSend(a){ () }.sync
}

trait Chan[A] extends Send[A] with Recv[A]

object Chan {
  def apply[A](capacity: Int = 0): Chan[A] = {
    require(capacity >= 0)
    capacity match {
      case 0 => new UnbufferedChan[A]
      case n => new BufferedChan[A](n)
    }
  }
}

class UnbufferedChan[A] extends Chan[A] { chan =>
  private val lock = new ReentrantLock
  private val txWaiters = new LinkedBlockingQueue[(A, () => Boolean)]
  private val rxWaiters = new LinkedBlockingQueue[A => Boolean]

  def onSend[B](a: A)(f: => B): Selection[B] = new Selection[B] {
    def lock(): Unit = chan.lock.lock()
    def unlock(): Unit = chan.lock.unlock()

    def isReady = !rxWaiters.isEmpty
    def select(): Option[() => B] = {
      var k: Option[() => B] = None
      while (!rxWaiters.isEmpty && k == None) {
        val rx = rxWaiters.remove()
        if (rx(a)) {
          k = Some(() => f)
        }
      }
      k
    }
    def register[BB >: B](cell: Cell[() => BB]): Unit = {
      txWaiters.add((a, () => cell.put(() => f)))
    }
  }

  def onRecv[B](f: A => B): Selection[B] = new Selection[B] {
    def lock(): Unit = chan.lock.lock()
    def unlock(): Unit = chan.lock.unlock()

    def isReady = !txWaiters.isEmpty
    def select(): Option[() => B] = {
      var k: Option[() => B] = None
      while (!txWaiters.isEmpty && k == None) {
        val (a, tx) = txWaiters.remove()
        if (tx()) {
          k = Some(() => f(a))
        }
      }
      k
    }
    def register[BB >: B](cell: Cell[() => BB]): Unit = {
      rxWaiters.add(a => cell.put(() => f(a)))
    }
  }
}

class BufferedChan[A](capacity: Int) extends Chan[A] { chan =>
  assert(capacity > 0)

  private val lock = new ReentrantLock
  private val txWaiters = new LinkedBlockingQueue[(A, () => Boolean)]
  private val buffer = new ArrayBlockingQueue[A](capacity)
  private val rxWaiters = new LinkedBlockingQueue[A => Boolean]

  def onSend[B](a: A)(f: => B): Selection[B] = new Selection[B] {
    def lock(): Unit = chan.lock.lock()
    def unlock(): Unit = chan.lock.unlock()

    def isReady = buffer.size < capacity || !rxWaiters.isEmpty
    def select(): Option[() => B] = {
      var sent = false
      if (buffer.size < capacity) {
        buffer.add(a)
        sent = true
      }
      while (!rxWaiters.isEmpty && !buffer.isEmpty) {
        val rx = rxWaiters.remove()
        if (rx(buffer.peek())) {
          buffer.remove()
          if (!sent) {
            buffer.add(a)
            sent = true
          }
        }
      }
      if (sent) {
        Some(() => f)
      } else {
        None
      }
    }
    def register[BB >: B](cell: Cell[() => BB]): Unit = {
      txWaiters.add((a, () => cell.put(() => f)))
    }
  }

  def onRecv[B](f: A => B): Selection[B] = new Selection[B] {
    def lock(): Unit = chan.lock.lock()
    def unlock(): Unit = chan.lock.unlock()

    def isReady = !buffer.isEmpty || !txWaiters.isEmpty
    def select(): Option[() => B] = {
      var recd: Option[A] = None
      if (!buffer.isEmpty) {
        recd = Some(buffer.remove())
      }
      while (!txWaiters.isEmpty && buffer.size < capacity) {
        val (a, tx) = txWaiters.remove()
        if (tx()) {
          buffer.add(a)
          if (recd.isEmpty) {
            recd = Some(buffer.remove())
          }
        }
      }
      recd.map(a => () => f(a))
    }
    def register[BB >: B](cell: Cell[() => BB]): Unit = {
      rxWaiters.add(a => cell.put(() => f(a)))
    }
  }
}

object Time {
  def after(duration: Duration): Recv[Unit] = {
    val chan = Chan[Unit](1)
    val task = new TimerTask {
      def run(): Unit = {
        chan.send(())
      }
    }
    Go.timer.schedule(task, duration.toMillis)
    chan
  }
}
