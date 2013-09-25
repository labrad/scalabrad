package org.labrad.util

import java.io.IOException
import java.net.{DatagramSocket, ServerSocket}

class Counter(min: Long, max: Long) {
  require(max >= min)
  val length = (max - min + 1)
  private var idx = 0L

  def next: Long = synchronized {
    val n = idx
    idx = (idx + 1) % length
    n + min
  }
}

object Util {
  def findAvailablePort(start: Int = 10000, stop: Int = 60000): Int = {
    for (port <- start to stop) {
      if (available(port)) return port
    }
    sys.error(s"could not find available port between $start and $stop")
  }

  /**
   * Checks to see if a specific port is available.
   * Based on implementation from stack overflow:
   * http://stackoverflow.com/questions/434718/sockets-discover-port-availability-using-java
   *
   * @param port the port to check for availability
   */
  def available(port: Int): Boolean = {
    var ss: ServerSocket = null
    var ds: DatagramSocket = null
    try {
      ss = new ServerSocket(port)
      ss.setReuseAddress(true)
      ds = new DatagramSocket(port)
      ds.setReuseAddress(true)
      true
    } catch {
      case e: IOException => false
    } finally {
      if (ds != null) {
        ds.close()
      }

      if (ss != null) {
        try {
          ss.close()
        } catch {
          case e: IOException => // should not be thrown
        }
      }
    }
  }
}
