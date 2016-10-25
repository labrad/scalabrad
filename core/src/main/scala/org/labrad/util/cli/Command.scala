package org.labrad.util.cli

import org.clapper.argot._
import org.clapper.argot.ArgotConverters._

/**
 * Base class for command line options.
 *
 * To add options for a command, create traits that can be mixed in to Command
 * and use the parser member to define additional command line options.
 */
class Command(name: String, doc: String = null) { self =>

  protected val parser: ArgotParser =
      new ArgotParser(name, preUsage = Option(doc))

  private val help = parser.flag[Boolean](List("h", "help"),
      "Print usage information and exit")

  private var parsed = false

  /**
   * Parse command line arguments. Returns self if parse was successful.
   * Raises ArgotUsageException if the command line parsing failed or the
   * -h or --help options were supplied.
   */
  def parse(args: Array[String]): self.type = synchronized {
    require(!parsed, "already parsed command line")
    parsed = true
    parser.parse(args)
    if (help.value.getOrElse(false)) parser.usage()
    self
  }
}
