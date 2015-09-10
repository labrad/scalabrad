package org.labrad.manager

import java.io.File
import java.net.URI
import java.nio.CharBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import org.clapper.argot._
import org.clapper.argot.ArgotConverters._
import org.labrad.annotations._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.registry._
import org.labrad.util._
import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}


trait AuthService {
  def authenticate(challenge: Array[Byte], response: Array[Byte]): Boolean
}

class AuthServiceImpl(password: Array[Char]) extends AuthService {
  def authenticate(challenge: Array[Byte], response: Array[Byte]): Boolean = {
    val md = MessageDigest.getInstance("MD5")
    md.update(challenge)
    md.update(UTF_8.encode(CharBuffer.wrap(password)))
    val expected = md.digest
    var same = expected.length == response.length
    for ((a, b) <- expected zip response) same = same & (a == b)
    same
  }
}


class CentralNode(port: Int, password: Array[Char], storeOpt: Option[RegistryStore]) extends Logging {
  // start services
  val tracker = new StatsTrackerImpl
  val hub: Hub = new HubImpl(tracker, () => messager)
  val messager: Messager = new MessagerImpl(hub, tracker)
  val auth: AuthService = new AuthServiceImpl(password)

  // Manager gets id 1L
  tracker.connectServer(Manager.ID, Manager.NAME)

  for (store <- storeOpt) {
    // Registry gets id 2L
    val name = "Registry"
    val id = hub.allocateServerId(name)
    val server = new Registry(id, name, store, hub, tracker)
    hub.connectServer(id, name, server)
  }

  // start listening for incoming network connections
  val listener = new Listener(auth, hub, tracker, messager, port)

  def stop() {
    listener.stop()
  }
}


object Manager extends Logging {
  val ID = 1L
  val NAME = "Manager"
  val DOC = "Provides basic support for all labrad connections, including discovery of other servers and lookup of metadata about them."

  // setting ids
  val SERVERS = 1L
  val SETTINGS = 2L
  val LOOKUP = 3L

  // first client id
  val ClientIdStart = 1000000000L

  // named messages
  case class Connect(id: Long, name: String, isServer: Boolean) extends Message {
    def msgName: String = "Connect"
    def msgData: Data = Cluster(UInt(id), Str(name), Bool(isServer))
  }

  case class Disconnect(id: Long, name: String, isServer: Boolean) extends Message {
    def msgName: String = "Disconnect"
    def msgData: Data = Cluster(UInt(id), Str(name), Bool(isServer))
  }

  case class ConnectServer(id: Long, name: String) extends Message {
    def msgName: String = "Server Connect"
    def msgData: Data = Cluster(UInt(id), Str(name))
  }

  case class DisconnectServer(id: Long, name: String) extends Message {
    def msgName: String = "Server Disconnect"
    def msgData: Data = Cluster(UInt(id), Str(name))
  }

  case class ExpireAll(id: Long) extends Message {
    def msgName: String = "Expire All"
    def msgData: Data = UInt(id)
  }

  case class ExpireContext(ctx: Context) extends Message {
    def msgName: String = "Expire Context"
    def msgData: Data = ctx.toData
  }

  def main(args: Array[String]) {

    val config = ManagerConfig.fromCommandLine(args) match {
      case Success(managerArgs) => managerArgs

      case Failure(e: ArgotUsageException) =>
        println(e.message)
        return

      case Failure(e: Throwable) =>
        println(s"unexpected error: $e")
        return
    }

    val storeOpt = config.registryUri.getScheme match {
      case null if config.registryUri == new URI("none") =>
        log.info("running with external registry")
        None

      case "file" =>
        val registry = new File(Util.bareUri(config.registryUri)).getAbsoluteFile

        def ensureDir(dir: File): Unit = {
          if (!dir.exists) {
            val ok = dir.mkdirs()
            if (!ok) sys.error(s"failed to create registry directory: $dir")
          }
        }

        val (store, format) = if (registry.isDirectory) {
          ensureDir(registry)
          config.registryUri.getQuery match {
            case null | "format=binary" => (new BinaryFileStore(registry), "binary")
            case "format=delphi" => (new DelphiFileStore(registry), "delphi")
            case query => sys.error(s"invalid format for registry directory: $query")
          }
        } else {
          ensureDir(registry.getParentFile)
          config.registryUri.getQuery match {
            case null | "format=sqlite" => (SQLiteStore(registry), "sqlite")
            case query => sys.error(s"invalid format for registry file: $query")
          }
        }
        log.info(s"registry location: $registry, format=$format")
        Some(store)

      case "labrad" =>
        val remoteHost = config.registryUri.getHost
        val remotePort = config.registryUri.getPort match {
          case -1 => 7682 // not specified; use default
          case port => port
        }
        val remotePassword = config.registryUri.getUserInfo match {
          case null => config.password
          case info => info.split(":") match {
            case Array() => config.password
            case Array(pw) => pw.toCharArray
            case Array(u, pw) => pw.toCharArray
          }
        }
        log.info(s"remote registry location: $remoteHost:$remotePort")
        Some(RemoteStore(remoteHost, remotePort, remotePassword))

      case scheme =>
        sys.error(s"unknown scheme for registry uri: $scheme. must use 'file', 'labrad'")
    }

    val centralNode = new CentralNode(config.port, config.password, storeOpt)

    // Optionally wait for EOF to stop the manager.
    // This is a convenience feature when developing in sbt, allowing the
    // manager to be stopped without killing sbt. However, this is generally
    // not desired when deployed; for example, start-stop-daemon detaches
    // from the process, so that stdin gets EOF, but we want the manager
    // to continue to run.
    val stopOnEOF = sys.props.get("org.labrad.stopOnEOF") == Some("true")
    if (stopOnEOF) {
      Util.awaitEOF()
      centralNode.stop()
    } else {
      sys.addShutdownHook {
        centralNode.stop()
      }
    }
  }
}

/**
 * Configuration for running the labrad manager.
 */
case class ManagerConfig(
  port: Int,
  password: Array[Char],
  registryUri: URI
)

object ManagerConfig {
  // helpers for dealing with paths
  implicit class PathString(path: String) {
    def / (file: String): File = new File(path, file)
  }

  implicit class PathFile(path: File) {
    def / (file: String): File = new File(path, file)
  }

  /**
   * Create ManagerConfig from command line and map of environment variables.
   *
   * @param args command line parameters
   * @param env map of environment variables, which defaults to the actual
   *        environment variables in scala.sys.env
   * @return a Try containing a ManagerArgs instance (on success) or a Failure
   *         in the case something went wrong. The Failure will contain an
   *         ArgotUsageException if the command line parsing failed or the
   *         -h or --help options were supplied.
   */
  def fromCommandLine(
    args: Array[String],
    env: Map[String, String] = scala.sys.env
  ): Try[ManagerConfig] = {
    val parser = new ArgotParser("labrad",
      preUsage = Some("The labrad manager."),
      sortUsage = false
    )
    val portOpt = parser.option[Int](
      names = List("port"),
      valueName = "int",
      description = "Port on which to listen for incoming connections. " +
        "If not provided, fallback to the value given in the LABRADPORT " +
        "environment variable, with default value 7682."
    )
    val passwordOpt = parser.option[String](
      names = List("password"),
      valueName = "string",
      description = "Password to use to authenticate incoming connections. " +
        "If not provided, fallback to the value given in the LABRADPASSWORD " +
        "environment variable, with default value '' (empty password)."
    )
    val registryOpt = parser.option[String](
      names = List("registry"),
      valueName = "uri",
      description = "URI giving the registry storage location. " +
        "Use labrad://[<pw>@]<host>[:<port>] to connect to a running manager, " +
        "or file://<path>[?format=<format>] to load data from a local file. " +
        "If the file path is a directory, we assume it is in the binary " +
        "one file per key format; if it points to a file, we assume it " +
        "is in the new SQLite format. The default format can be overridden " +
        "by adding a 'format=' query parameter to the URI, with the following " +
        "options: binary (one file per key with binary data), " +
        "delphi (one file per key with legacy delphi text-formatted data), " +
        "sqlite (single sqlite file for entire registry). If not provided, " +
        "fallback to the value given in the LABRADREGISTRY environment " +
        "variable, with the default being to store registry values in SQLite " +
        "format in $HOME/.labrad/registry.sqlite"
    )
    val help = parser.flag[Boolean](List("h", "help"),
      "Print usage information and exit")

    Try {
      parser.parse(ArgParsing.expandLongArgs(args))
      if (help.value.getOrElse(false)) parser.usage()

      val port = portOpt.value.orElse(env.get("LABRADPORT").map(_.toInt)).getOrElse(7682)
      val password = passwordOpt.value.orElse(env.get("LABRADPASSWORD")).getOrElse("").toCharArray
      val registryUri = registryOpt.value.orElse(env.get("LABRADREGISTRY")).map(new URI(_)).getOrElse {
        (sys.props("user.home") / ".labrad" / "registry.sqlite").toURI
      }

      ManagerConfig(port, password, registryUri)
    }
  }
}
