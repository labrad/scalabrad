package org.labrad.util

import java.io.IOException
import java.net.{DatagramSocket, ServerSocket}

class Counter(min: Long, max: Long) {
  require(max >= min)
  val length = (max - min + 1)
  require(min + (length-1) == max, s"cannot create range from $min to $max")

  private var idx = 0L

  def next: Long = synchronized {
    val n = idx
    idx = (idx + 1) % length
    n + min
  }
}

class ShapeIterator(shape: Array[Int]) extends Iterator[Array[Int]] {
  private val nDims = shape.size
  private val indices = Array.ofDim[Int](nDims)
  indices(nDims - 1) = -1

  private var _hasNext = shape.forall(_ > 0)

  def hasNext: Boolean = _hasNext

  def next: Array[Int] = {
    // increment indices
    var k = nDims - 1
    while (k >= 0) {
      indices(k) += 1
      if (indices(k) == shape(k)) {
        indices(k) = 0
        k -= 1
      } else {
        k = -1 // done
      }
    }
    // check if this is the last iteration
    var last = true
    k = nDims - 1
    while (k >= 0) {
      if (indices(k) == shape(k) - 1) {
        k -= 1
      } else {
        last = false
        k = -1 // done
      }
    }
    _hasNext = !last
    indices
  }
}

object Util {
  /**
   * Reads from stdin until EOF is reached, e.g. due to Ctrl-D.
   *
   * This consumes and discards any input on stdin and blocks the calling
   * thread; use with care.
   */
  def awaitEOF(): Unit = {
    var done = false
    while (!done) {
      if (System.in.read() == -1) {
        done = true
      }
    }
  }

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

  /**
   * Parse an array of command-line arguments against the given allowed options.
   *
   * For example, if we specify "foo" and "bar" as allowed options, then we
   * will accept command line arguments of the form "--foo=x" or "--bar=y".
   * Any arguments not of this form will raise an exception.
   *
   * Returns a Map from allowed options to the provided values.
   */
  def parseArgs(args: Array[String], allowed: Seq[String]): Map[String, String] = {
    def parseArg(arg: String, opt: String): Option[String] = {
      val prefix = s"--$opt="
      if (arg.startsWith(prefix)) {
        Some(arg.stripPrefix(prefix))
      } else {
        None
      }
    }

    args.map { arg =>
      val values = for {
        opt <- allowed
        value <- parseArg(arg, opt)
      } yield {
        (opt, value)
      }
      values.headOption.getOrElse {
        sys.error(s"unknown argument: $arg")
      }
    }.toMap
  }
}
