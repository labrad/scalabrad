package org.labrad.manager

import io.netty.bootstrap.Bootstrap
import io.netty.channel.{ChannelInitializer, ChannelOption, EventLoopGroup}
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import java.nio.ByteOrder
import org.labrad._
import org.labrad.annotations._
import org.labrad.concurrent.{Chan, Send, Time}
import org.labrad.concurrent.Go._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.registry.RegistryStore
import org.labrad.types._
import org.labrad.util.Futures._
import org.labrad.util.Logging
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

trait LocalServer2 {
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

class LocalServerActor2(server: LocalServer, hub: Hub, tracker: StatsTracker)(implicit ec: ExecutionContext)
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

class ServerConnector(name: String, registry: RegistryStore, externalConfig: ServerConfig)
                     (implicit ec: ExecutionContext) extends Logging {
  private val servers = mutable.Map.empty[(String, Int), ServerConnection]

  // connect to managers that are stored in the registry
  refresh()

  def list(): Seq[(String, Int, Boolean)] = synchronized {
    servers.toSeq.map { case ((host, port), conn) =>
      (host, port, conn.isConnected)
    }.sorted
  }

  def refresh(): Unit = synchronized {
    // load the list of managers from the registry
    // TODO: need a node-specific path in the registry
    var dir = registry.root
    dir = registry.child(dir, "Servers", create = true)
    val default = DataBuilder("*(sws)").array(0).result()
    val result = registry.getValue(dir, "External", default = Some((true, default)))
    val configs = result.get[Seq[(String, Long, String)]].map {
      case (host, port, pw) =>
        externalConfig.copy(
          host = host,
          port = if (port != 0) port.toInt else externalConfig.port,
          credential = if (pw != "") Password("", pw.toCharArray) else externalConfig.credential
        )
    }
    val urls = configs.map(c => s"${c.host}:${c.port}")
    log.info(s"external servers in registry: ${urls.mkString(", ")}")

    // connect to any servers we are not already connected to
    for (config <- configs) {
      servers.getOrElseUpdate((config.host, config.port), {
        new ServerConnection(config)
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
    servers.getOrElseUpdate((config.host, config.port), {
      new ServerConnection(config)
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
    }
  }

  def drop(hostPat: String, port: Int = 0): Unit = synchronized {
    for ((key, connector) <- matchingManagers(hostPat, port.toInt)) {
      connector.stop()
      servers.remove(key)
    }
  }

  private def matchingManagers(hostPat: String, port: Int): Seq[((String, Int), RemoteConnector)] = {
    val hostRegex = hostPat.r
    for {
      ((h, p), connector) <- servers.toSeq
      if hostRegex.unapplySeq(h).isDefined
      if port == 0 || port == p
    } yield {
      ((h, p), connector)
    }
  }
}

class ServerConnection(
  config: ServerConfig,
  auth: AuthService,
  hub: Hub,
  tracker: StatsTracker,
  messager: Messager,
  tlsHostConfig: TlsHostConfig,
  authTimeout: Duration,
  registryTimeout: Duration,
  workerGroup: EventLoopGroup
)(implicit ec: ExecutionContext) extends Logging {

  implicit val timeout = 30.seconds
  val reconnectDelay = 10.seconds
  val pingDelay = 30.seconds
  val addr = s"${config.host}:${config.port}"

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
      log.info(s"$addr: connecting...")

      val chan = try {
        val chan = connect()
        connected = true
        log.info(s"$addr: connected")
        chan
      } catch {
        case e: Exception =>
          log.error(s"$addr: failed to connect", e)
          null
      }

      def doClose(): Unit = {
        try {
          chan.close()
        } catch {
          case e: Exception =>
            log.error(s"$addr: error during connection close", e)
        }
      }

      while (connected) {
        select(
          chan.closeFuture.toScala.onRecv { _ =>
            log.info(s"$addr: connection lost; will reconnect after $reconnectDelay")
            connected = false
          },
          ctrl.onRecv {
            case Ping =>

            case Reconnect =>
              doClose()
              log.info(s"$addr: will reconnect after $reconnectDelay")
              connected = false

            case Stop =>
              doClose()
              log.info(s"$addr: stopped")
              connected = false
              done = true
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

  private def connect(): NioSocketChannel = {
    val b = new Bootstrap()
    b.group(workerGroup)
     .channel(classOf[NioSocketChannel])
     .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
     .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
     .handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          val p = ch.pipeline
//          if (tls == TlsMode.ON) {
//            p.addLast("sslContext", makeSslContext(host).newHandler(ch.alloc(), host, port))
//          }
          p.addLast("packetCodec", new PacketCodec(forceByteOrder = ByteOrder.BIG_ENDIAN))
          p.addLast("loginHandler", new LoginHandler(auth, hub, tracker, messager, tlsHostConfig,
              TlsPolicy.OFF, authTimeout, registryTimeout))
//          p.addLast("packetHandler", new SimpleChannelInboundHandler[Packet] {
//            override protected def channelRead0(ctx: ChannelHandlerContext, packet: Packet): Unit = {
//              handlePacket(packet)
//            }
//            override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
//              closeNoWait(cause)
//            }
//            override def channelInactive(ctx: ChannelHandlerContext): Unit = {
//              closeNoWait(new Exception("channel closed"))
//            }
//          })
        }
     })

    b.connect(config.host, config.port).sync().channel.asInstanceOf[NioSocketChannel]
  }
}
