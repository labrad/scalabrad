package org.labrad.util.cli

import org.clapper.argot._
import org.clapper.argot.ArgotConverters._

/**
 * Base class for command line options.
 *
 * To add options for a command, create traits that can be mixed in to Command and use the parser
 * member to define additional command line options.
 */
class Command(name: String, doc: String = null) { self =>

  protected val parser: ArgotParser = new ArgotParser(name, preUsage = Option(doc))

  private val help = parser.flag[Boolean](List("h", "help"), "Print usage information and exit")

  private var parsed = false

  /**
   * Parse command line arguments. Returns self if parse was successful. Throws ArgotUsageException
   * if the command line parsing failed or the -h or --help options were supplied.
   */
  def parse(args: Array[String]): self.type = synchronized {
    require(!parsed, "already parsed command line")
    parsed = true
    parser.parse(args)
    if (help.value.getOrElse(false)) parser.usage()
    self
  }
}

/**
 * Holder for a map of environment variables.
 *
 * We use this instead of a plain Map[String, String] when passing the environment as an implicit
 * variable since Map is a rather common type. This class itself implements Map[String, String], by
 * forwarding calls to the underlying map instance.
 */
case class Environment(map: Map[String, String]) {
  def empty: Environment = Environment(Map.empty[String, String])
  def get(key: String): Option[String] = map.get(key)
  def iterator: Iterator[(String, String)] = map.iterator
  //def + [B >: String](kv: (String, B)): Map[String, B] = map + kv
  //def - (key: String): Environment = Environment(map - key)
}

object Environment {

  /**
   * Convenience constructor to build an environment from a sequence of key-value tuples.
   */
  def apply(kvs: (String, String)*): Environment = new Environment(Map(kvs: _*))

  /**
   * Wrapper for the standard environment variables from scala.sys
   */
  lazy val sys = Environment(scala.sys.env)
}
