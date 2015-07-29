package org.labrad.util

import java.io.IOException
import java.net.{DatagramSocket, ServerSocket, URI}
import java.util.regex.Pattern

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
   * Parse a command line arg into a boolean value.
   *
   * true values: on, yes, y, true, t, 1
   * false values: off, no, n, false, f, 0
   */
  def parseBooleanOpt(arg: String): Boolean = {
    arg.toLowerCase match {
      case "on" | "yes" | "y" | "true" | "t" | "1" => true
      case "off" | "no" | "n" | "false" | "f" | "0" => false
    }
  }

  /**
   * Drop the query and fragment from a URI.
   */
  def bareUri(uri: URI): URI = {
    new URI(uri.getScheme, uri.getHost, uri.getPath, uri.getFragment)
  }

  /**
   * Interpolate environment variables in the given string.
   *
   * Tokens of the form %ENV_VAR_NAME% in the string will be replaced with the value of
   * ENV_VAR_NAME in the given environment map.
   *
   * @param str The string to interpolate
   * @param env Map from environment variable names to values. Defaults to the system environment.
   */
  def interpolateEnvironmentVars(str: String, env: Map[String, String] = scala.sys.env): String = {
    var result: String = str

    // find all environment vars in the string
    val p = Pattern.compile("%([^%]*)%")
    val m = p.matcher(str)
    val keys = {
      val keys = Set.newBuilder[String]
      while (m.find) keys += m.group(1)
      keys.result
    }

    // substitute environment variable into string
    for (key <- keys.toSeq) {
      require(env.contains(key), s"Cannot interpolate '$str': '$key' not defined in environment")
      result = result.replaceAll("%" + key + "%", env(key))
    }

    result
  }
}
