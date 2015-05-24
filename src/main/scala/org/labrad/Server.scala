package org.labrad

import java.io.{PrintWriter, StringWriter}
import org.labrad.data._
import org.labrad.errors.LabradException
import org.labrad.util.{AsyncSemaphore, Logging, Util}
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Success, Failure}

abstract class Server[S <: Server[S, _] : TypeTag, T <: ServerContext : TypeTag] extends Logging {

  private var cxn: ServerConnection = _
  private implicit def ec: ExecutionContext = cxn.executionContext

  private val contexts = mutable.Map.empty[Context, ContextState]

  private val (srvSettings, srvHandlerFactory) = Reflect.makeHandler[S]
  private val (ctxSettings, ctxHandlerFactory) = Reflect.makeHandler[T]
  private val settings = srvSettings ++ ctxSettings
  private val srvHandler = srvHandlerFactory(this.asInstanceOf[S])

  private val srvSettingIds = srvSettings.map(_.id).toSet
  private val ctxSettingIds = ctxSettings.map(_.id).toSet

  private[labrad] def doInit(cxn: ServerConnection): Unit = {
    this.cxn = cxn

    init(cxn)

    val msgId = cxn.getMessageId
    cxn.addMessageListener {
      case Message(`msgId`, context, _, _) =>
        val stateOpt = contexts.synchronized { contexts.remove(context) }
        stateOpt.foreach { _.expire() }
    }

    val registerF = for {
      _ <- registerSettings()
      _ <- cxn.send("Manager", "S: Notify on Context Expiration" -> Cluster(UInt(msgId), Bool(false)))
      _ <- cxn.send("Manager", "S: Start Serving" -> Data.NONE)
    } yield ()
    Await.result(registerF, 30.seconds)

    log.info("Now serving...")
  }

  private def registerSettings(): Future[Unit] = {
    val registrations = settings.sortBy(_.id).map(s =>
      Cluster(
        UInt(s.id),
        Str(s.name),
        Str(s.doc),
        Arr(s.accepts.strs.map(Str(_))), // TODO: when manager supports patterns, just use .pat here
        Arr(s.returns.strs.map(Str(_))),
        Str(""))
    )
    cxn.send("Manager", registrations.map("S: Register Setting" -> _): _*).map(_ => ())
  }


  private[labrad] def doShutdown(): Unit = {
    val states = contexts.synchronized {
      val states = contexts.values.toVector
      contexts.clear()
      states
    }
    val expirations = states.map { _.expire() }
    Await.result(Future.sequence(expirations), 60.seconds)
    shutdown()
  }

  // user-overidden methods
  def init(cxn: ServerConnection): Unit
  def shutdown(): Unit

  def newContext(cxn: ServerConnection, context: Context): T

  /**
   * Standard entry point for running a server.
   *
   * Includes standard parsing of command line options.
   */
  def run(args: Array[String]): Unit = {
    val options = Util.parseArgs(args, Seq("host", "port", "password"))

    val host = options.get("host").orElse(sys.env.get("LABRADHOST")).getOrElse("localhost")
    val port = options.get("port").orElse(sys.env.get("LABRADPORT")).map(_.toInt).getOrElse(7682)
    val password = options.get("password").orElse(sys.env.get("LABRADPASSWORD")).getOrElse("").toCharArray

    val cxn = ServerConnection[S, T](this, host, port, password)
    cxn.connect()
    sys.ShutdownHookThread(cxn.triggerShutdown)
    cxn.serve()
  }


  class ContextState {
    val sem: AsyncSemaphore = new AsyncSemaphore(1)
    var state: Option[T] = None
    var handler: RequestContext => Data = _
    var inited: Boolean = false

    def expire()(implicit ec: ExecutionContext): Future[Unit] = sem.map {
      state.foreach(_.expire())
    }
  }

  def handleRequest(packet: Packet): Future[Packet] = {
    val contextState = contexts.synchronized {
      contexts.getOrElseUpdate(packet.context, new ContextState)
    }
    contextState.sem.map {
      Server.handle(packet) { r =>
        if (srvSettingIds.contains(r.id)) {
          srvHandler(r)
        } else if (ctxSettingIds.contains(r.id)) {
          if (!contextState.inited) {
            val state = newContext(cxn, packet.context)
            val handler = ctxHandlerFactory(state)
            state.init()
            contextState.state = Some(state)
            contextState.handler = handler
            contextState.inited = true
          }
          contextState.handler(r)
        } else {
          Error(1, s"Setting not found: ${r.id}")
        }
      }
    }
  }

  def get(context: Context): Option[T] = {
    contexts.synchronized {
      contexts.get(context).flatMap(_.state)
    }
  }
}

trait ServerContext {
  def init(): Unit
  def expire(): Unit
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
}
