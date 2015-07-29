package org.labrad

import java.io.{File, PrintWriter, StringWriter}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Pattern
import org.clapper.argot._
import org.clapper.argot.ArgotConverters._
import org.labrad.annotations.IsServer
import org.labrad.data._
import org.labrad.errors.LabradException
import org.labrad.util.{ArgParsing, AsyncSemaphore, Logging, Util}
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Success, Failure, Try}

trait ServerContext {
  def init(): Unit
  def expire(): Unit
}

abstract class Server[S <: Server[S, _] : TypeTag, T <: ServerContext : TypeTag] extends Logging {

  val name: String
  val doc: String

  private var _cxn: ServerConnection = _
  private val shutdownCalled = new AtomicBoolean(false)

  /**
   * Allow subclasses to access (but not modify) the server connection
   */
  protected def cxn: ServerConnection = _cxn

  protected val contexts = mutable.Map.empty[Context, ContextState]

  private val (srvSettings, srvHandlerFactory) = Reflect.makeHandler[S]
  private val (ctxSettings, ctxHandlerFactory) = Reflect.makeHandler[T]
  private val settings = srvSettings ++ ctxSettings
  private val srvHandler = srvHandlerFactory(this.asInstanceOf[S])

  private val srvSettingIds = srvSettings.map(_.id).toSet
  private val ctxSettingIds = ctxSettings.map(_.id).toSet

  /**
   * Start this server by connecting to the manager at the given location.
   */
  def start(
    host: String,
    port: Int,
    password: Array[Char],
    nameOpt: Option[String] = None,
    tls: TlsMode = TlsMode.STARTTLS,
    tlsCerts: Map[String, File] = Map()
  ): Unit = {
    val name = Util.interpolateEnvironmentVars(nameOpt.getOrElse(this.name))
    _cxn = new ServerConnection(name, doc, host, port, password, tls, tlsCerts, packet => handleRequest(packet))
    cxn.connect()

    // if the vm goes down or we lose the connection, shutdown
    sys.ShutdownHookThread { stop() }
    cxn.addConnectionListener { case false => stop() }

    init()

    val msgId = cxn.getMessageId
    val msgCtx = cxn.newContext
    cxn.addMessageListener {
      case Message(`msgId`, `msgCtx`, _, Cluster(UInt(high), UInt(low))) =>
        val context = Context(high, low)
        val stateOpt = contexts.synchronized { contexts.remove(context) }
        stateOpt.foreach { state =>
          state.expire().onFailure { case e =>
            log.error(s"error expiring $context", e)
          }
        }
    }

    val registrations = settings.sortBy(_.id).map(s =>
      Cluster(
        UInt(s.id),
        Str(s.name),
        Str(s.doc),
        Arr(s.accepts.strs.map(Str(_))), // TODO: when manager supports patterns, just use .pat here
        Arr(s.returns.strs.map(Str(_))),
        Str(""))
    )

    val registerF = for {
      _ <- cxn.send("Manager", registrations.map("S: Register Setting" -> _): _*)
      _ <- cxn.send("Manager", "S: Notify on Context Expiration" -> Cluster(UInt(msgId), Bool(false)))
      _ <- cxn.send("Manager", "S: Start Serving" -> Data.NONE)
    } yield ()
    Await.result(registerF, 30.seconds)

    log.info("Now serving...")
  }

  /**
   * A promise that will be completed when this server shuts down.
   */
  private val shutdownPromise = Promise[Unit]

  /**
   * Stop this server
   */
  def stop(): Unit = {
    if (shutdownCalled.compareAndSet(false, true)) {
      try {
        val ctxs = contexts.synchronized {
          val ctxs = contexts.values.toVector
          contexts.clear()
          ctxs
        }
        val expirations = ctxs.map { _.expire() }
        Await.result(Future.sequence(expirations), 10.seconds)
        shutdown()
        cxn.close()
        shutdownPromise.trySuccess(())
      } catch {
        case e: Exception =>
          shutdownPromise.tryFailure(e)
      }
    }
  }

  /**
   * Wait for this server to shutdown
   */
  def awaitShutdown(timeout: Duration = Duration.Inf): Unit = {
    Await.result(shutdownPromise.future, timeout)
  }

  // user-overidden methods
  def init(): Unit
  def shutdown(): Unit
  def newContext(context: Context): T


  /**
   * Helper class containing per-context state and a semaphore to enable
   * serializing requests made within a given context.
   */
  class ContextState {
    val sem: AsyncSemaphore = new AsyncSemaphore(1)
    var handler: Option[(T, RequestContext => Data)] = None

    def expire(): Future[Unit] = sem.map {
      expireNoLock()
    }

    def expireNoLock(): Unit = {
      handler.foreach { case (state, _) => state.expire() }
      handler = None
    }
  }

  private def handleRequest(packet: Packet): Future[Packet] = {
    val contextState = contexts.synchronized {
      contexts.getOrElseUpdate(packet.context, new ContextState)
    }
    contextState.sem.map {
      Server.handle(packet) { r =>
        if (srvSettingIds.contains(r.id)) {
          srvHandler(r)
        } else if (ctxSettingIds.contains(r.id)) {
          val handler = contextState.handler match {
            case None =>
              val state = newContext(packet.context)
              val handler = ctxHandlerFactory(state)
              state.init()
              contextState.handler = Some((state, handler))
              handler

            case Some((state, handler)) =>
              handler
          }
          handler(r)
        } else {
          Error(1, s"Setting not found: ${r.id}")
        }
      }
    }
  }

  /**
   * Allow looking up the context object for another context.
   *
   * This is needed in some cases for cross-context communication.
   */
  protected def get(context: Context): Option[T] = {
    contexts.synchronized {
      contexts.get(context).flatMap(_.handler).map { case (state, _) => state}
    }
  }

  /**
   * Expire the given context.
   *
   * Does not attempt to acquire the context lock, so this can and should be
   * used only if the lock is already being held, for example while handling
   * a request in this context. Note that the setting method must have a
   * different name from this function, in order not to confuse the
   */
  protected def doExpireContext(context: Context): Unit = {
    contexts.synchronized {
      contexts.get(context).foreach(_.expireNoLock())
    }
  }
}

object Server {
  def handle(packet: Packet, includeStackTrace: Boolean = true)(f: RequestContext => Data): Packet = {
    val Packet(request, source, context, records) = packet

    val in = records.iterator
    val out = Seq.newBuilder[Record]
    var error = false
    while (in.hasNext && !error) {
      val Record(id, data) = in.next
      val resp = try {
        f(RequestContext(source, context, id, data))
      } catch {
        case ex: LabradException =>
          ex.toData

        case ex: Throwable =>
          val msg = if (includeStackTrace) {
            val sw = new StringWriter
            ex.printStackTrace(new PrintWriter(sw))
            sw.toString
          } else {
            ex.getMessage
          }
          Error(0, msg)
      }
      if (resp.isError) {
        error = true
      }
      out += Record(id, resp)
    }

    Packet(-request, source, context, out.result)
  }

  def handleAsync(packet: Packet)(f: RequestContext => Future[Data])(implicit ec: ExecutionContext): Future[Packet] = {
    val Packet(request, source, context, records) = packet

    def process(records: Seq[Record]): Future[Seq[Record]] = records match {
      case Seq(Record(id, data), tail @ _*) =>
        f(RequestContext(source, context, id, data)) recover {
          case ex =>
            val sw = new StringWriter
            ex.printStackTrace(new PrintWriter(sw))
            Error(0, sw.toString)
        } flatMap {
          case resp if resp.isError =>
            Future.successful { Record(id, resp) +: Seq() }
          case resp =>
            process(tail) map { Record(id, resp) +: _ }
        }

      case Seq() =>
        Future.successful(Seq())
    }
    process(records) map {
      Packet(-request, source, context, _)
    }
  }

  /**
   * Standard entry point for running a server.
   *
   * Includes basic parsing of command line options.
   */
  def run(s: Server[_, _], args: Array[String]): Unit = {
    val config = ServerConfig.fromCommandLine(args) match {
      case Success(config) => config

      case Failure(e: ArgotUsageException) =>
        println(e.message)
        return

      case Failure(e: Throwable) =>
        println(s"unexpected error: $e")
        return
    }
    s.start(
      config.host,
      if (config.tls == TlsMode.ON) config.tlsPort else config.port,
      config.password,
      config.nameOpt
    )
  }
}

/**
 * Configuration for running a labrad server.
 */
case class ServerConfig(
  host: String,
  port: Int,
  password: Array[Char],
  nameOpt: Option[String],
  tls: TlsMode,
  tlsPort: Int
)

object ServerConfig {
  /**
   * Create ServerConfig from command line and map of environment variables.
   *
   * @param args command line parameters
   * @param env map of environment variables, which defaults to the actual
   *        environment variables in scala.sys.env
   * @return a Try containing a ServerArgs instance (on success) or a Failure
   *         in the case something went wrong. The Failure will contain an
   *         ArgotUsageException if the command line parsing failed or the
   *         -h or --help options were supplied.
   */
  def fromCommandLine(
    args: Array[String],
    env: Map[String, String] = scala.sys.env
  ): Try[ServerConfig] = {
    val parser = new ArgotParser("labrad-server",
      preUsage = Some("Run a labrad server."),
      sortUsage = false
    )
    val nameOpt = parser.option[String](
      names = List("name"),
      valueName = "string",
      description = "The name to use for this server when connecting to the " +
        "labrad manager. If given, this will override the default name. " +
        "This is generally not needed, but can be useful in testing if you " +
        "want to run several copies of a given server without name conflicts."
    )
    val hostOpt = parser.option[String](
      names = List("host"),
      valueName = "string",
      description = "The hostname where the labrad manager is running. " +
        "If not provided, fallback to the value given in the LABRADHOST " +
        "environment variable, with default value 'localhost'."
    )
    val portOpt = parser.option[Int](
      names = List("port"),
      valueName = "int",
      description = "Port on which to connect to the labrad manager. " +
        "If not provided, fallback to the value given in the LABRADPORT " +
        "environment variable, with default value 7682."
    )
    val tlsOpt = parser.option[String](
      names = List("tls"),
      valueName = "string",
      description = "The TLS mode to use for secure connections to the " +
        "labrad manager. Valid options are 'on', 'off', 'starttls', and " +
        "'starttls-force'. If not provided, fallback to the value given in " +
        "the LABRAD_TLS environment variable, with default value 'starttls'. " +
        "If 'on', we connect to the manager's tls-port and encrypt the " +
        "connection with TLS from the start. If 'starttls', the connection " +
        "is initially unencrypted, but will then be upgraded to use TLS, " +
        "unless connecting to a manager running on localhost; " +
        "'starttls-force' will upgrade to TLS, even when connecting to " +
        "localhost. Finally, 'off' will disable TLS entirely; this is " +
        "recommended only for use with legacy managers."
    )
    val tlsPortOpt = parser.option[Int](
      names = List("tls-port"),
      valueName = "int",
      description = "Port on which to connect to the labrad manager with " +
        "TLS. If not provided, fallback to the value given in the " +
        "LABRAD_TLS_PORT environment variable, with default value 7643."
    )
    val passwordOpt = parser.option[String](
      names = List("password"),
      valueName = "string",
      description = "Password to use to authenticate to the manager. " +
        "If not provided, fallback to the value given in the LABRADPASSWORD " +
        "environment variable, with default value '' (empty password)."
    )
    val help = parser.flag[Boolean](List("h", "help"),
      "Print usage information and exit")

    Try {
      parser.parse(ArgParsing.expandLongArgs(args))
      if (help.value.getOrElse(false)) parser.usage()

      val host = hostOpt.value.orElse(env.get("LABRADHOST")).getOrElse("localhost")
      val port = portOpt.value.orElse(env.get("LABRADPORT").map(_.toInt)).getOrElse(7682)
      val tls = tlsOpt.value.orElse(env.get("LABRAD_TLS")).map(TlsMode.fromString).getOrElse(TlsMode.STARTTLS)
      val tlsPort = tlsPortOpt.value.orElse(env.get("LABRAD_TLS_PORT").map(_.toInt)).getOrElse(7643)
      val password = passwordOpt.value.orElse(env.get("LABRADPASSWORD")).getOrElse("").toCharArray

      ServerConfig(host, port, password, nameOpt.value, tls, tlsPort)
    }
  }
}
