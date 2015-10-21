package org.labrad.manager

import io.netty.handler.ssl.{SslContext, SslContextBuilder}
import io.netty.handler.ssl.util.SelfSignedCert
import io.netty.util.DomainNameMapping
import java.io.File
import java.net.{InetAddress, URI}
import java.nio.CharBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.security.{MessageDigest, SecureRandom}
import java.nio.file.Files
import org.clapper.argot._
import org.clapper.argot.ArgotConverters._
import org.labrad.TlsMode
import org.labrad.annotations._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.registry._
import org.labrad.util._
import org.labrad.util.Paths._
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


class CentralNode(
  password: Array[Char],
  storeOpt: Option[RegistryStore],
  listeners: Seq[(Int, TlsPolicy)],
  tlsConfig: TlsHostConfig
) extends Logging {
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
  val listener = new Listener(auth, hub, tracker, messager, listeners, tlsConfig)

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
        val TlsModeParam = "tls=(.+)".r
        val tls = config.registryUri.getQuery match {
          case null => TlsMode.STARTTLS
          case TlsModeParam(mode) => TlsMode.fromString(mode)
          case query => sys.error(s"invalid params for registry config: $query")
        }
        log.info(s"remote registry location: $remoteHost:$remotePort, tls=$tls")
        Some(RemoteStore(remoteHost, remotePort, remotePassword, tls))

      case scheme =>
        sys.error(s"unknown scheme for registry uri: $scheme. must use 'file', 'labrad'")
    }

    val tlsPolicy = (config.tlsRequired, config.tlsRequiredLocalhost) match {
      case (false, _)    => TlsPolicy.STARTTLS_OPT
      case (true, false) => TlsPolicy.STARTTLS
      case (true, true)  => TlsPolicy.STARTTLS_FORCE
    }
    val listeners = Seq(
      config.port -> tlsPolicy,
      config.tlsPort -> TlsPolicy.ON
    )

    // By default, make a self-signed cert for the hostname of this machine.
    // If the config includes this hostname as well, that will override the
    // default configuration.
    val hostname = InetAddress.getLocalHost.getHostName()
    val hostMap = Map(hostname -> CertConfig.SelfSigned) ++ config.tlsHosts

    val hosts = hostMap.toSeq.map {
      case (host, CertConfig.SelfSigned) =>
        host -> sslContextForHost(host,
                                  certPath = config.tlsCertPath,
                                  keyPath = config.tlsKeyPath)

      case (host, CertConfig.Files(cert, key, None)) =>
        host -> (cert, SslContextBuilder.forServer(cert, key).build())

      case (host, CertConfig.Files(cert, key, Some(interm))) =>
        // concatenate cert and intermediates files
        val allCerts = File.createTempFile("labrad-manager", "cert")
        Files.write(allCerts.toPath,
          Files.readAllBytes(cert.toPath) ++ Files.readAllBytes(interm.toPath))
        allCerts.deleteOnExit()
        host -> (cert, SslContextBuilder.forServer(allCerts, key).build())
    }
    val default = sslContextForHost("localhost",
                                    certPath = config.tlsCertPath,
                                    keyPath = config.tlsKeyPath)
    val tlsHostConfig = TlsHostConfig(default, hosts: _*)

    val centralNode = new CentralNode(config.password, storeOpt, listeners, tlsHostConfig)

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

  /**
   * Create an SSL/TLS context for the given host, using self-signed certificates.
   */
  private def sslContextForHost(host: String, certPath: File, keyPath: File): (File, SslContext) = {
    val certFile = certPath / s"${host}.cert"
    val keyFile = keyPath / s"${host}.key"

    if (!certFile.exists() || !keyFile.exists()) {
      // if one exists but not the other, we have a problem
      require(!certFile.exists(), s"found cert file $certFile but no matching key file $keyFile")
      require(!keyFile.exists(), s"found key file $keyFile but no matching cert file $certFile")

      log.info(s"Generating self-signed certificate for host '$host'. certFile=$certFile, keyFile=$keyFile")
      val ssc = SelfSignedCert(host, bits = 2048)(secureRandom)
      copy(ssc.certificate, certFile)
      copy(ssc.privateKey, keyFile)
    } else {
      log.info(s"Using saved certificate for host '$host'. certFile=$certFile, keyFile=$keyFile")
    }

    (certFile, SslContextBuilder.forServer(certFile, keyFile).build())
  }

  // A shared SecureRandom instance. Only created if needed because entropy collection is costly.
  private lazy val secureRandom = new SecureRandom()

  /**
   * Copy the given source file to the specified destination, creating
   * destination directories as needed.
   */
  private def copy(src: File, dst: File): Unit = {
    ensureDir(dst.getParentFile)
    Files.copy(src.toPath, dst.toPath)
  }

  /**
   * Create directories as needed to ensure that the specified location exists.
   */
  private def ensureDir(dir: File): Unit = {
    if (!dir.exists) {
      val ok = dir.mkdirs()
      if (!ok) sys.error(s"failed to create directory: $dir")
    }
  }
}

/**
 * Configuration for running the labrad manager.
 */
case class ManagerConfig(
  port: Int,
  password: Array[Char],
  registryUri: URI,
  tlsPort: Int,
  tlsRequired: Boolean,
  tlsRequiredLocalhost: Boolean,
  tlsHosts: Map[String, CertConfig],
  tlsCertPath: File,
  tlsKeyPath: File
)

sealed trait CertConfig

object CertConfig {
  case object SelfSigned extends CertConfig
  case class Files(
    cert: File,
    key: File,
    intermediates: Option[File] = None
  ) extends CertConfig
}

object ManagerConfig {

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
        "Use labrad://[<pw>@]<host>[:<port>][?tls=<mode>] to connect to a running manager, " +
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
    val tlsPortOpt = parser.option[Int](
      names = List("tls-port"),
      valueName = "int",
      description = "Port on which to listen for incoming TLS connections. " +
        "If not provided, fallback to the value given in the LABRAD_TLS_PORT " +
        "environment variable, with default value 7643."
    )
    val tlsRequiredOpt = parser.option[String](
      names = List("tls-required"),
      valueName = "bool",
      description = "Whether to require TLS for incoming connections from " +
        "remote hosts. If not provided, fallback to the value given in the " +
        "LABRAD_TLS_REQUIRED environment variable, with default value true."
    )
    val tlsRequiredLocalhostOpt = parser.option[String](
      names = List("tls-required-localhost"),
      valueName = "bool",
      description = "Whether to require TLS for incoming connections from " +
        "localhost. If not provided, fallback to the value given in the " +
        "LABRAD_TLS_REQUIRED_LOCALHOST environment variable, with default " +
        "value false."
    )
    val tlsHostsOpt = parser.option[String](
      names = List("tls-hosts"),
      valueName = "string",
      description = "A list of hostnames for which to use TLS. " +
        "Clients can use server name indication (SNI) to specify which " +
        "hostname they are connecting to when negotiating TLS, and the " +
        "manager must use the appropriate certificate and key for each" +
        "hostname. The config is a semicolon-separated list of hosts, where " +
        "for each host we have either just <hostname>, in which case the " +
        "manager will generate a self-signed certificate for this hostname, " +
        "or <hostname>?cert=<cert-file>&key=<key-file>[&intermediates=<intermediates-file>], " +
        "in which case the manager will use the given cert and key files " +
        "(and optional file containing intermediate certs) for this " +
        "hostname. If not provided, fallback to the value given in the " +
        "LABRAD_TLS_HOSTS environment variable, we the default being to " +
        "generate a single self-signed certificate for the 'localhost'."
    )
    val tlsCertPathOpt = parser.option[String](
      names = List("tls-cert-path"),
      valueName = "directory",
      description = "Path to a directory where self-signed TLS certificates " +
        "should be stored. If not given, fall back to the value in the " +
        "LABRAD_TLS_CERT_PATH environment variable, with default value " +
        "$HOME/.labrad/manager/certs. Within this directory, we will store " +
        "one cert file for each hostname configured to use self-signed certs " +
        "in the tls-hosts option, named <hostname>.cert. If this directory " +
        "does not exist, it will be created."
    )
    val tlsKeyPathOpt = parser.option[String](
      names = List("tls-key-path"),
      valueName = "directory",
      description = "Path to a directory where keys for self-signed TLS " +
        "certificates should be stored. If not given, fall back to the value " +
        "in the LABRAD_TLS_KEY_PATH environment variable, with default value " +
        "$HOME/.labrad/manager/keys. Within this directory, we will store " +
        "one key file for each hostname configured to use self-signed certs " +
        "in the tls-hosts option, named <hostname>.key. If this directory " +
        "does not exist, it will be created."
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
      val tlsPort = tlsPortOpt.value.orElse(env.get("LABRAD_TLS_PORT").map(_.toInt)).getOrElse(7643)
      val tlsRequired = tlsRequiredOpt.value.orElse(env.get("LABRAD_TLS_REQUIRED")).map(Util.parseBooleanOpt).getOrElse(true)
      val tlsRequiredLocalhost = tlsRequiredLocalhostOpt.value.orElse(env.get("LABRAD_TLS_REQUIRED_LOCALHOST")).map(Util.parseBooleanOpt).getOrElse(false)
      val tlsHosts = parseTlsHostsConfig(tlsHostsOpt.value.orElse(env.get("LABRAD_TLS_HOSTS")).getOrElse(""))
      val tlsCertPath = tlsCertPathOpt.value.orElse(env.get("LABRAD_TLS_CERT_PATH")).map(new File(_)).getOrElse {
        sys.props("user.home") / ".labrad" / "manager" / "certs"
      }
      val tlsKeyPath = tlsKeyPathOpt.value.orElse(env.get("LABRAD_TLS_KEY_PATH")).map(new File(_)).getOrElse {
        sys.props("user.home") / ".labrad" / "manager" / "keys"
      }

      ManagerConfig(
        port,
        password,
        registryUri,
        tlsPort,
        tlsRequired,
        tlsRequiredLocalhost,
        tlsHosts,
        tlsCertPath,
        tlsKeyPath)
    }
  }

  /**
   * Parse a string representing the tls host configuration.
   *
   * The config is a semicolon-separated list of hosts, where for each host we
   * have either just:
   * <hostname> (will use self-signed certificates for this hostname), or
   * <hostname>?cert=<cert-file>&key=<key-file>[&intermediates=<intermediates-file>].
   *
   * For example, if we had the string:
   *
   * public.com?cert=/etc/ssl/certs/public.crt&key=/etc/ssl/private/public.key;private;private2
   *
   * Then we would configure TLS to use the given certificates in /etc/ssl for
   * connections made to the hostname public.com, and our own self-signed certs
   * for connections made to hostnames private and private2.
   */
  def parseTlsHostsConfig(hostsConfig: String): Map[String, CertConfig] = {
    if (hostsConfig.isEmpty) {
      Map()
    } else {
      hostsConfig.split(";").map(parseTlsHost).toMap
    }
  }

  /**
   * Parse hostname config for a single TLS host.
   */
  def parseTlsHost(hostConfig: String): (String, CertConfig) = {
    require(hostConfig != "")

    hostConfig.split('?') match {
      case Array(host) =>
        (host, CertConfig.SelfSigned)

      case Array(host, paramStr) =>
        val paramMap = paramStr.split('&').map { param =>
          param.split('=') match { case Array(k, v) => k -> v }
        }.toMap

        val unknownParams = paramMap.keys.toSet -- Seq("cert", "intermediates", "key")
        require(unknownParams.isEmpty, s"unknown parameters for host $host: ${unknownParams.mkString(", ")}")

        val params = (paramMap.get("cert"),
                      paramMap.get("key"),
                      paramMap.get("intermediates"))

        val certConfig = params match {
          case (Some(cert), Some(key), None) =>
            CertConfig.Files(cert = new File(cert), key = new File(key))

          case (Some(cert), Some(key), Some(interm)) =>
            CertConfig.Files(
              cert = new File(cert),
              key = new File(key),
              intermediates = Some(new File(interm))
            )

          case (None, None, None) =>
            CertConfig.SelfSigned

          case _ =>
            sys.error(s"must specify both cert and key for host $host")
        }

        (host, certConfig)
    }
  }
}
