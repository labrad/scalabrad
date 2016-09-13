package org.labrad.manager

import io.netty.handler.ssl.{SslContext, SslContextBuilder}
import io.netty.handler.ssl.util.SelfSignedCert
import io.netty.util.DomainNameMapping
import java.io.File
import java.net.{InetAddress, URI}
import java.nio.CharBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.security.{MessageDigest, SecureRandom}
import org.clapper.argot._
import org.clapper.argot.ArgotConverters._
import org.labrad.{Password, ServerConfig, ServerInfo, TlsMode}
import org.labrad.annotations._
import org.labrad.concurrent.ExecutionContexts
import org.labrad.crypto.{CertConfig, Certs}
import org.labrad.data._
import org.labrad.errors._
import org.labrad.manager.auth._
import org.labrad.registry._
import org.labrad.util._
import org.labrad.util.Paths._
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Await}
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
  regStoreOpt: Option[RegistryStore],
  authStoreOpt: Option[AuthStore],
  oauthClients: Map[OAuthClientType, OAuthClientInfo],
  listeners: Seq[(Int, TlsPolicy)],
  tlsConfig: TlsHostConfig,
  authTimeout: Duration,
  registryTimeout: Duration
) extends Logging {

  val bossGroup = Listener.newBossGroup()
  val workerGroup = Listener.newWorkerGroup()
  val loginGroup = Listener.newLoginGroup()

  val nettyExecutionContext = ExecutionContext.fromExecutor(workerGroup)

  // start services
  val tracker = new StatsTrackerImpl
  val hub: Hub = new HubImpl(tracker, () => messager)(nettyExecutionContext)
  val messager: Messager = new MessagerImpl(hub, tracker)
  val auth: AuthService = new AuthServiceImpl(password)

  // Manager gets id 1L
  tracker.connectServer(Manager.ID, Manager.NAME)

  // TODO: this should be configurable, for example passwords per host
  val externalConfig = ServerConfig(host = "", port = 0, credential = Password("", password))

  // ExecutionContext for potentially long-running server work (auth server and registry)
  lazy val serversExecutionContext =
    ExecutionContexts.newCachedThreadExecutionContext("ServerWorkers")

  for (regStore <- regStoreOpt) {
    val name = Registry.NAME
    val id = hub.allocateServerId(name)
    val registry = new Registry(id, name, regStore, externalConfig)(serversExecutionContext)
    hub.setServerInfo(ServerInfo(registry.id, registry.name, registry.doc, registry.settings))
    hub.connectServer(id, name, new LocalServerActor(registry, hub, tracker))
  }

  for (authStore <- authStoreOpt) {
    val name = Authenticator.NAME
    val id = hub.allocateServerId(name)
    val regStore = regStoreOpt.getOrElse {
      sys.error("cannot run auth server without direct registry access")
    }
    val verifierOpt = if (oauthClients.isEmpty) {
      None
    } else {
      Some(new OAuthVerifier(oauthClients))
    }
    val auth = new AuthServer(id, name, hub, authStore, verifierOpt, regStore, externalConfig)(serversExecutionContext)
    hub.setServerInfo(ServerInfo(auth.id, auth.name, auth.doc, auth.settings))
    hub.connectServer(id, name, new LocalServerActor(auth, hub, tracker))
  }

  // start listening for incoming network connections
  val listener = new Listener(
      auth, hub, tracker, messager, listeners, tlsConfig,
      authTimeout = authTimeout, registryTimeout = registryTimeout,
      bossGroup = bossGroup, workerGroup = workerGroup, loginGroup = loginGroup)

  def stop() {
    listener.stop()
    loginGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
  }
}


object Manager extends Logging {
  val VERSION = {
    val url = getClass.getResource("/org/labrad/version.txt")
    scala.io.Source.fromURL(url).mkString
  }
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

  // A shared SecureRandom instance. Only created if needed because entropy collection is costly.
  private implicit lazy val secureRandom = new SecureRandom()

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

    val regStoreOpt = config.registryUri.getScheme match {
      case null if config.registryUri == new URI("none") =>
        log.info("running with external registry")
        None

      case "file" =>
        val registry = new File(Util.bareUri(config.registryUri)).getAbsoluteFile

        val (store, format) = if (registry.isDirectory) {
          Util.ensureDir(registry)
          config.registryUri.getQuery match {
            case null | "format=binary" => (new BinaryFileStore(registry), "binary")
            case "format=delphi" => (new DelphiFileStore(registry), "delphi")
            case query => sys.error(s"invalid format for registry directory: $query")
          }
        } else {
          Util.ensureDir(registry.getParentFile)
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
        val (remoteUsername, remotePassword) = config.registryUri.getUserInfo match {
          case null => ("", config.password)
          case info => info.split(":") match {
            case Array() => ("", config.password)
            case Array(pw) => ("", pw.toCharArray)
            case Array(u, pw) => (u, pw.toCharArray)
          }
        }
        val TlsModeParam = "tls=(.+)".r
        val tls = config.registryUri.getQuery match {
          case null => TlsMode.STARTTLS
          case TlsModeParam(mode) => TlsMode.fromString(mode)
          case query => sys.error(s"invalid params for registry config: $query")
        }
        log.info(s"remote registry location: $remoteHost:$remotePort, tls=$tls")
        Some(RemoteStore(remoteHost, remotePort, Password(remoteUsername, remotePassword), tls))

      case scheme =>
        sys.error(s"unknown scheme for registry uri: $scheme. must use 'file', 'labrad'")
    }

    val authStoreOpt = if (config.authServer) {
      Some(AuthStore(file = config.authUsersFile))
    } else {
      None
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
        host -> Certs.sslContextForHost(host,
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
    val default = Certs.sslContextForHost("localhost",
                                          certPath = config.tlsCertPath,
                                          keyPath = config.tlsKeyPath)
    val tlsHostConfig = TlsHostConfig(default, hosts: _*)

    log.info(s"localhost: sha1=${Certs.fingerprintSHA1(default._1)}")
    for ((host, (cert, _)) <- hosts.toSeq.sortBy(_._1)) {
      log.info(s"$host: sha1=${Certs.fingerprintSHA1(cert)}")
    }

    val centralNode = new CentralNode(
      password = config.password,
      regStoreOpt = regStoreOpt,
      authStoreOpt = authStoreOpt,
      oauthClients = config.oauthClients,
      listeners = listeners,
      tlsConfig = tlsHostConfig,
      authTimeout = config.authTimeout,
      registryTimeout = config.registryTimeout)

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
  registryTimeout: Duration,
  registryUri: URI,
  tlsPort: Int,
  tlsRequired: Boolean,
  tlsRequiredLocalhost: Boolean,
  tlsHosts: Map[String, CertConfig],
  tlsCertPath: File,
  tlsKeyPath: File,
  authServer: Boolean,
  authTimeout: Duration,
  authUsersFile: File,
  oauthClients: Map[OAuthClientType, OAuthClientInfo]
)

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
    val registryTimeout = parser.option[Int](
      names = List("registry-timeout"),
      valueName = "seconds",
      description = "Time in seconds to wait for registry server to connect " +
        "when clients or servers attempt to log in. If running an internal " +
        "registry server, this has no effect. The default is 30 seconds."
    )
    val authServerOpt = parser.option[String](
      names = List("auth-server"),
      valueName = "bool",
      description = "Whether to run auth server locally and store database " +
        "of username and password info. If true, the default, the auth " +
        "server will be run locally and a local user database file will be " +
        "used. Otherwise, an external auth server must connect to this " +
        "manager to provide username+password or OAuth authentication. " +
        "If not provided, fallback to the value in the LABRAD_AUTH_SERVER " +
        "environment variable."
    )
    val authTimeout = parser.option[Int](
      names = List("auth-timeout"),
      valueName = "seconds",
      description = "Time in seconds to wait for auth server to connect " +
        "and for auth requests to complete when clients or servers attempt " +
        "to login with extended auth methods such as oauth_token. The " +
        "default is 30 seconds."
    )
    val oauthClientIdOpt = parser.option[String](
      names = List("oauth-client-id"),
      valueName = "string",
      description = "Client ID to use for authenticating users with OAuth. " +
        "If not given, fallback to the value in the LABRAD_OAUTH_CLIENT_ID " +
        "environment variable."
    )
    val oauthClientSecretOpt = parser.option[String](
      names = List("oauth-client-secret"),
      valueName = "string",
      description = "Client Secret to use for authenticating users with OAuth. " +
        "If not given, fallback to the value in the LABRAD_OAUTH_CLIENT_SECRET " +
        "environment variable."
    )
    val oauthWebClientIdOpt = parser.option[String](
      names = List("oauth-web-client-id"),
      valueName = "string",
      description = "Client ID to use for authenticating users with OAuth on " +
        "the web. If not given, fallback to the value in the LABRAD_OAUTH_WEB_CLIENT_ID " +
        "environment variable."
    )
    val oauthWebClientSecretOpt = parser.option[String](
      names = List("oauth-web-client-secret"),
      valueName = "string",
      description = "Client Secret to use for authenticating users with OAuth on " +
        "the web. If not given, fallback to the value in the LABRAD_OAUTH_WEB_CLIENT_SECRET " +
        "environment variable."
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
      parser.parse(args)
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
        CertConfig.Defaults.tlsCertPath
      }
      val tlsKeyPath = tlsKeyPathOpt.value.orElse(env.get("LABRAD_TLS_KEY_PATH")).map(new File(_)).getOrElse {
        CertConfig.Defaults.tlsKeyPath
      }

      val oauthClients = Map.newBuilder[OAuthClientType, OAuthClientInfo]
      val oauthClientId = oauthClientIdOpt.value.orElse(env.get("LABRAD_OAUTH_CLIENT_ID"))
      val oauthClientSecret = oauthClientSecretOpt.value.orElse(env.get("LABRAD_OAUTH_CLIENT_SECRET"))
      (oauthClientId, oauthClientSecret) match {
        case (None, None) =>
        case (Some(id), Some(secret)) =>
          oauthClients += OAuthClientType.Desktop -> OAuthClientInfo(id, secret)
        case _ =>
          sys.error("Must specify both or neither of oauth client id and client secret.")
      }
      val oauthWebClientId = oauthWebClientIdOpt.value.orElse(env.get("LABRAD_OAUTH_WEB_CLIENT_ID"))
      val oauthWebClientSecret = oauthWebClientSecretOpt.value.orElse(env.get("LABRAD_OAUTH_WEB_CLIENT_SECRET"))
      (oauthWebClientId, oauthWebClientSecret) match {
        case (None, None) =>
        case (Some(id), Some(secret)) =>
          oauthClients += OAuthClientType.Web -> OAuthClientInfo(id, secret)
        case _ =>
          sys.error("Must specify both or neither of oauth web client id and web client secret.")
      }

      ManagerConfig(
        port,
        password,
        registryTimeout = registryTimeout.value.getOrElse(30).seconds,
        registryUri,
        tlsPort,
        tlsRequired,
        tlsRequiredLocalhost,
        tlsHosts,
        tlsCertPath,
        tlsKeyPath,
        authServer = authServerOpt.value.orElse(env.get("LABRAD_AUTH_SERVER")).map(Util.parseBooleanOpt).getOrElse(true),
        authTimeout = authTimeout.value.getOrElse(30).seconds,
        authUsersFile = sys.props("user.home") / ".labrad" / "users.sqlite",
        oauthClients = oauthClients.result
      )
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
