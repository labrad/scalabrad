package org.labrad.util

import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong

object NettyUtil {
  /**
   * Create an EventLoopGroup with the specified number of threads.
   *
   * The threads will be grouped and named like "NAME-GROUP-NUM"
   * where NAME is the supplied name, GROUP is an integer count
   * coming from the supplied groupCounter, and NUM is a count
   * of threads within this event loop group itself.
   *
   * Specifying 0 for numThreads, the default, will create an
   * EventLoopGroup with a default number of threads, based on
   * the available parallelism (number of cores) on the machine.
   */
  def newEventLoopGroup(
    name: String,
    groupCounter: AtomicLong,
    numThreads: Int = 0
  ): EventLoopGroup = {
    val groupCount = groupCounter.getAndIncrement()
    val threadGroup = new ThreadGroup(s"$name-$groupCount")

    val threadFactory = new ThreadFactory {
      val counter = new AtomicLong(0)
      def newThread(r: Runnable): Thread = {
        val i = counter.getAndIncrement()
        val thread = new Thread(threadGroup, r, s"$name-$groupCount-$i")
        thread.setDaemon(false)
        thread
      }
    }

    new NioEventLoopGroup(numThreads, threadFactory)
  }
}
