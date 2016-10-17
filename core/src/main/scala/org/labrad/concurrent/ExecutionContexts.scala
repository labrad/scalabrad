package org.labrad.concurrent

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}

object ExecutionContexts {

  /**
   * Create an ExecutionContext backed by a cached thread pool.
   * All threads will be created in a group and named with the given name.
   */
  def newCachedThreadExecutionContext(name: String): ExecutionContext = {
    val threadGroup = new ThreadGroup(name)
    val threadFactory = new ThreadFactory {
      val counter = new AtomicLong(0)
      def newThread(r: Runnable): Thread = {
        val i = counter.getAndIncrement()
        val thread = new Thread(threadGroup, r, s"$name$i")
        thread.setDaemon(true)
        thread
      }
    }
    val executor = Executors.newCachedThreadPool(threadFactory)
    ExecutionContext.fromExecutorService(executor)
  }
}
