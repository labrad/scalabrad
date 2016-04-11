package org.labrad.manager

import org.labrad._
import org.labrad.annotations._
import org.labrad.concurrent.{Chan, Send, Time}
import org.labrad.concurrent.Go._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.registry.RegistryStore
import org.labrad.types._
import org.labrad.util.Logging
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

trait LocalServer {
  def id: Long
  def name: String
  def doc: String
  def settings: Seq[SettingInfo]

  def message(src: String, packet: Packet): Unit
  def request(src: String, packet: Packet, messageFunc: (Long, Packet) => Unit)(implicit timeout: Duration): Future[Packet]
  def expireContext(src: String, context: Context)(implicit timeout: Duration): Future[Long]
  def expireAll(src: String, high: Long)(implicit timeout: Duration): Future[Long]
  def expireAll(src: String)(implicit timeout: Duration): Future[Unit]
}

class LocalServerActor(server: LocalServer, hub: Hub, tracker: StatsTracker)(implicit ec: ExecutionContext)
extends ServerActor {
  val username = ""// Local servers running in manager process act like global user
  val srcId = ""
  private val messageFunc = (target: Long, pkt: Packet) => hub.message(target, pkt)

  def message(packet: Packet): Unit = {
    tracker.msgRecv(server.id)
    server.message(srcId, packet)
  }

  def request(packet: Packet)(implicit timeout: Duration): Future[Packet] = {
    tracker.serverReq(server.id)
    val f = server.request(srcId, packet, messageFunc)
    f.onComplete { _ =>
      tracker.serverRep(server.id)
    }
    f
  }

  def expireContext(ctx: Context)(implicit timeout: Duration): Future[Long] = server.expireContext(srcId, ctx)
  def expireAll(high: Long)(implicit timeout: Duration): Future[Long] = server.expireAll(srcId, high)

  def close(): Unit = {}
}

class MultiheadServer(name: String, registry: RegistryStore, server: LocalServer, externalConfig: ServerConfig)
                     (implicit ec: ExecutionContext) extends Logging {
  private val managers = mutable.Map.empty[(String, Int), RemoteConnector]

  // connect to managers that are stored in the registry
  refresh()

  def list(): Seq[(String, Int, Boolean)] = synchronized {
    managers.toSeq.map { case ((host, port), reg) =>
      (host, port, reg.isConnected)
    }.sorted
  }

  def refresh(): Unit = synchronized {
    // load the list of managers from the registry
    var dir = registry.root
    dir = registry.child(dir, "Servers", create = true)
    dir = registry.child(dir, name, create = true)
    dir = registry.child(dir, "Multihead", create = true)
    val default = DataBuilder("*(sws)").array(0).result()
    val (result, textOpt) = registry.getValue(dir, "Managers", default = Some((true, default)))
    val configs = result.get[Seq[(String, Long, String)]].map {
      case (host, port, pw) =>
        externalConfig.copy(
          host = host,
          port = if (port != 0) port.toInt else externalConfig.port,
          credential = if (pw != "") Password("", pw.toCharArray) else externalConfig.credential
        )
    }
    val urls = configs.map(c => s"${c.host}:${c.port}")
    log.info(s"managers in registry: ${urls.mkString(", ")}")

    // connect to any managers we are not already connected to
    for (config <- configs) {
      managers.getOrElseUpdate((config.host, config.port), {
        new RemoteConnector(server, config)
      })
    }
  }

  def add(host: String, port: Option[Int], password: Option[String]): Unit = synchronized {
    val config = externalConfig.copy(
      host = host,
      port = port.getOrElse(externalConfig.port),
      credential = password.map { pw =>
        Password("", pw.toCharArray)
      }.getOrElse(externalConfig.credential)
    )
    managers.getOrElseUpdate((config.host, config.port), {
      new RemoteConnector(server, config)
    })
  }

  def ping(hostPat: String = ".*", port: Int = 0): Unit = synchronized {
    for ((_, connector) <- matchingManagers(hostPat, port)) {
      connector.ping()
    }
  }

  def reconnect(hostPat: String, port: Int = 0): Unit = synchronized {
    for ((key, connector) <- matchingManagers(hostPat, port.toInt)) {
      connector.reconnect()
      managers.remove(key)
    }
  }

  def drop(hostPat: String, port: Int = 0): Unit = synchronized {
    for ((key, connector) <- matchingManagers(hostPat, port.toInt)) {
      connector.stop()
      managers.remove(key)
    }
  }

  private def matchingManagers(hostPat: String, port: Int): Seq[((String, Int), RemoteConnector)] = {
    val hostRegex = hostPat.r
    for {
      ((h, p), connector) <- managers.toSeq
      if hostRegex.unapplySeq(h).isDefined
      if port == 0 || port == p
    } yield {
      ((h, p), connector)
    }
  }
}

class RemoteConnector(server: LocalServer, config: ServerConfig)(implicit ec: ExecutionContext) extends Logging {

  implicit val timeout = 30.seconds
  val reconnectDelay = 10.seconds
  val pingDelay = 30.seconds
  val srcId = s"${config.host}:${config.port}"

  class ServerProxy(stopped: Send[Unit]) extends IServer {
    val name = server.name
    val doc = server.doc
    val settings = server.settings

    private var cxn: Connection = _
    private var messageFunc: (Long, Packet) => Unit = _

    def connected(cxn: Connection, ec: ExecutionContext): Unit = {
      this.cxn = cxn
      messageFunc = (target: Long, pkt: Packet) => {
        val msg = Request(target, pkt.context, pkt.records)
        try {
          cxn.sendMessage(msg)
        } catch {
          case e: Exception =>
            log.error(s"$srcId: error while sending message", e)
        }
      }
    }
    def handleRequest(packet: Packet): Future[Packet] = {
      server.request(srcId, packet, messageFunc)
    }
    def expire(context: Context): Unit = server.expireContext(srcId, context)
    def stop(): Unit = {
      server.expireAll(srcId)
      stopped.send(())
    }
  }

  @volatile private var connected = false
  def isConnected = connected

  sealed trait Msg
  case object Ping extends Msg
  case object Reconnect extends Msg
  case object Stop extends Msg

  private val ctrl = Chan[Msg](1)

  private val runFuture = go {
    var done = false
    while (!done) {
      log.info(s"$srcId: connecting...")
      val disconnected = Chan[Unit](1)
      val server = new ServerProxy(disconnected)

      val cxn: Connection = try {
        val cxn = Server.start(server, config)
        connected = true
        log.info(s"$srcId: connected")
        cxn
      } catch {
        case e: Exception =>
          log.error(s"$srcId: failed to connect", e)
          null
      }

      def doPing(): Unit = {
        log.info(s"$srcId: ping")
        val mgr = new ManagerServerProxy(cxn)
        mgr.echo(Str("ping")).onFailure {
          case e: Exception =>
            log.error(s"$srcId: error during ping", e)
        }
      }

      def doClose(): Unit = {
        try {
          cxn.close()
        } catch {
          case e: Exception =>
            log.error(s"$srcId: error during connection close", e)
        }
      }

      while (connected) {
        select(
          disconnected.onRecv { _ =>
            log.info(s"$srcId: connection lost; will reconnect after $reconnectDelay")
            connected = false
          },
          ctrl.onRecv {
            case Ping =>
              doPing()

            case Reconnect =>
              doClose()
              log.info(s"$srcId: will reconnect after $reconnectDelay")
              connected = false

            case Stop =>
              doClose()
              log.info(s"$srcId: stopped")
              connected = false
              done = true
          },
          Time.after(pingDelay).onRecv { _ =>
            doPing()
          }
        )
      }

      if (!done) {
        select(
          Time.after(reconnectDelay).onRecv { _ => },
          ctrl.onRecv {
            case Ping | Reconnect =>
            case Stop => done = true
          }
        )
      }
    }
  }

  def ping(): Unit = {
    ctrl.send(Ping)
  }

  def reconnect(): Unit = {
    ctrl.send(Reconnect)
  }

  def stop(): Future[Unit] = {
    ctrl.send(Stop)
    runFuture
  }
}
