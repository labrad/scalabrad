package org.labrad.util

import org.clapper.argot.{ArgotParser, ArgotUsageException}
import org.clapper.argot.ArgotConverters._

object ArgParsing {
  /**
   * Regular expression to match a gnu-style long command line argument
   * --argname=value. Captures the long argument name and value.
   */
  val LongArgWithValue = """(--[A-Za-z0-9_][A-Za-z0-9_\-]*)=(.*)""".r

  /**
   * Process an array of command line arguments into a form that Argot can
   * handle. Most args are passed through unchanged, except that GNU-style
   * long arguments of the form "--arg=value" are expanded into two separate
   * arguments "--arg" and "value".
   */
  def expandLongArgs(args: Array[String]): Array[String] = {
    args.flatMap {
      case LongArgWithValue(arg, value) => Array(arg, value)
      case arg => Array(arg)
    }
  }
}
