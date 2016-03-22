package org.labrad

import io.netty.bootstrap.Bootstrap
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelOption, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.ssl.{SslContext, SslContextBuilder}
import java.io.{File, IOException}
import java.net.{InetAddress, InetSocketAddress}
import java.nio.{ByteOrder, CharBuffer}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.security.MessageDigest
import java.util.concurrent.{ExecutionException, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import org.labrad.data._
import org.labrad.errors._
import org.labrad.events.MessageListener
import org.labrad.manager.Manager
import org.labrad.util.{Counter, LookupProvider}
import org.labrad.util.Futures._
import org.labrad.util.Paths._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/**
 * Modes of operation for securing client connections with TLS.
 */
sealed trait TlsMode

object TlsMode {
  case object OFF extends TlsMode
  case object STARTTLS extends TlsMode
  case object STARTTLS_FORCE extends TlsMode
  case object ON extends TlsMode

  def fromString(s: String): TlsMode = {
    s.toLowerCase match {
      case "on" => ON
      case "off" => OFF
      case "starttls" => STARTTLS
      case "starttls-force" => STARTTLS_FORCE
      case _ => sys.error(s"Invalid tls mode '$s'. Expected 'on', 'off', or 'starttls'.")
    }
  }
}

/**
 * A client or server connection to the Labrad manager.
 */
trait Connection {

  val name: String
  val host: String
  val port: Int
  val tls: TlsMode
  val tlsCerts: Map[String, File]
  def password: Array[Char]

  var id: Long = _
  var loginMessage: String = _

  private var isConnected = false
  def connected = isConnected
  private def connected_=(state: Boolean): Unit = { isConnected = state }

  protected var connectionListeners: List[PartialFunction[Boolean, Unit]] = Nil
  protected var messageListeners: List[PartialFunction[Message, Unit]] = Nil

  def addConnectionListener(listener: PartialFunction[Boolean, Unit]): Unit = {
    connectionListeners ::= listener
  }
  def removeConnectionListener(listener: PartialFunction[Boolean, Unit]): Unit = {
    connectionListeners = connectionListeners filterNot (_ == listener)
  }

  def addMessageListener(listener: PartialFunction[Message, Unit]): Unit = {
    messageListeners ::= listener
  }
  def removeMessageListener(listener: PartialFunction[Message, Unit]): Unit = {
    messageListeners = messageListeners filterNot (_ == listener)
  }

  private val nextMessageId = new AtomicInteger(1)
  def getMessageId = nextMessageId.getAndIncrement()

  // TODO: inject event loop group from outside so it can be shared between connections.
  // This will mean that its lifecycle must be managed separately, since we won't be
  // able to shut it down ourselves.
  private val workerGroup: NioEventLoopGroup = new NioEventLoopGroup()
  protected val lookupProvider = new LookupProvider(this.send)
  protected val requestDispatcher = new RequestDispatcher(sendPacket)
  implicit val executionContext = ExecutionContext.fromExecutor(workerGroup)

  private var channel: Channel = _

  private def certsDirectory = sys.props("user.home") / ".labrad" / "client" / "certs"
  private def certFile(hostname: String): File = certsDirectory / s"${hostname}.cert"

  /**
   * Create an SSL context for the given hostname.
   *
   * We look for trusted certificate files first in the tlsCerts map, then
   * in the default location on disk. If no appropriate certificate is found
   * in either of those locations, the resulting SSL context will instead use
   * the system-default trust roots to validate the server certificate, which
   * will only work if the server uses a certificate issued by a well-known
   * certificate authority.
   */
  private def makeSslContext(hostname: String): SslContext = {
    val sslContextBuilder = SslContextBuilder.forClient()
    val file = tlsCerts.get(hostname) match {
      case Some(file) => file
      case None => certFile(hostname)
    }
    if (file.exists) {
      sslContextBuilder.trustManager(file)
    }
    sslContextBuilder.build()
  }

  def connect(): Unit = {
    val b = new Bootstrap()
    b.group(workerGroup)
     .channel(classOf[NioSocketChannel])
     .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
     .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
     .handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          val p = ch.pipeline
          if (tls == TlsMode.ON) {
            p.addLast("sslContext", makeSslContext(host).newHandler(ch.alloc(), host, port))
          }
          p.addLast("packetCodec", new PacketCodec(forceByteOrder = ByteOrder.BIG_ENDIAN))
          p.addLast("packetHandler", new SimpleChannelInboundHandler[Packet] {
            override protected def channelRead0(ctx: ChannelHandlerContext, packet: Packet): Unit = {
              handlePacket(packet)
            }
            override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
              closeNoWait(cause)
            }
            override def channelInactive(ctx: ChannelHandlerContext): Unit = {
              closeNoWait(new Exception("channel closed"))
            }
          })
        }
     })

    channel = b.connect(host, port).sync().channel

    connected = true

    try {
      val isLocalConnection = (channel.remoteAddress, channel.localAddress) match {
        case (remote: InetSocketAddress, local: InetSocketAddress) =>
          remote.getAddress.isLoopbackAddress || remote.getAddress == local.getAddress

        case _ => false
      }

      val useStartTls = tls match {
        case TlsMode.STARTTLS_FORCE => true
        case TlsMode.STARTTLS if !isLocalConnection => true
        case _ => false
      }

      if (useStartTls) {
        val startTls = Request(Manager.ID, records = Seq(Record(1, Cluster(Str("STARTTLS"), Str(host)))))
        val Str(cert) = Await.result(send(startTls), 10.seconds)(0)

        var handler = makeSslContext(host).newHandler(channel.alloc(), host, port)
        channel.pipeline.addFirst("sslHandler", handler)
        Await.result(handler.handshakeFuture.toScala, 10.seconds)
      }

      doLogin(password)
    } catch {
      case e: Throwable => close(e); throw e
    }

    for (listener <- connectionListeners) listener.lift(true)
  }

  private def doLogin(password: Array[Char]): Unit = {
    try {
      // send first ping packet; response is password challenge
      val Bytes(challenge) = Await.result(send(Request(Manager.ID)), 10.seconds)(0)

      val md = MessageDigest.getInstance("MD5")
      md.update(challenge)
      md.update(UTF_8.encode(CharBuffer.wrap(password)))
      val data = Bytes(md.digest)

      // send password response; response is welcome message
      val Str(msg) = try {
        Await.result(send(Request(Manager.ID, records = Seq(Record(0, data)))), 10.seconds)(0)
      } catch {
        case e: ExecutionException => throw new IncorrectPasswordException
      }
      loginMessage = msg

      // send identification packet; response is our assigned connection id
      val UInt(assignedId) = Await.result(send(Request(Manager.ID, records = Seq(Record(0, loginData)))), 10.seconds)(0)
      id = assignedId

    } catch {
      case e: InterruptedException => throw new LoginFailedException(e)
      case e: ExecutionException => throw new LoginFailedException(e)
      case e: IOException => throw new LoginFailedException(e)
    }
  }

  protected def loginData: Data

  def close(): Unit = close(new IOException("Connection closed."))

  private def close(cause: Throwable): Unit = {
    closeNoWait(cause)

    if (channel != null) {
      channel.close().sync()
    }
    workerGroup.shutdownGracefully(10, 100, TimeUnit.MILLISECONDS).sync()
  }

  private def closeNoWait(cause: Throwable): Unit = {
    val listeners = synchronized {
      if (connected) {
        connected = false
        requestDispatcher.failAll(cause)

        channel.close()
        workerGroup.shutdownGracefully(10, 100, TimeUnit.MILLISECONDS)

        connectionListeners.map(_.lift)
      } else {
        Seq()
      }
    }
    for (listener <- listeners) listener(false)
  }

  def send(target: String, records: (String, Data)*): Future[Seq[Data]] =
    send(target, Context(0, 0), records: _*)

  def send(target: String, context: Context, records: (String, Data)*): Future[Seq[Data]] = {
    val recs = for ((name, data) <- records) yield NameRecord(name, data)
    send(NameRequest(target, context, recs))
  }

  def send(request: NameRequest): Future[Seq[Data]] =
    lookupProvider.resolve(request).flatMap(send)

  def send(request: Request): Future[Seq[Data]] = {
    require(connected, "Not connected.")
    requestDispatcher.startRequest(request)
  }

  def sendMessage(request: NameRequest): Unit =
    lookupProvider.resolve(request).map(r => sendMessage(r))

  def sendMessage(request: Request): Unit =
    sendPacket(Packet.forMessage(request))

  protected def sendPacket(packet: Packet): Unit = {
    require(connected, "Not connected.")
    channel.writeAndFlush(packet)
  }


  private val contextCounter = new Counter(0, 0xFFFFFFFFL)
  def newContext = Context(0, contextCounter.next)

  protected def handlePacket(packet: Packet): Unit = packet match {
    case Packet(id, _, _, _) if id > 0 => handleRequest(packet)
    case Packet( 0, _, _, _)           => handleMessage(packet)
    case Packet(id, _, _, _) if id < 0 => handleResponse(packet)
  }

  protected def handleResponse(packet: Packet): Unit = {
    requestDispatcher.finishRequest(packet)
  }

  protected def handleMessage(packet: Packet): Unit = {
    executionContext.execute(new Runnable {
      def run: Unit = {
        for (Record(id, data) <- packet.records) {
          val message = Message(packet.target, packet.context, id, data)
          messageListeners.foreach(_.lift(message))
        }
      }
    })
  }

  protected def handleRequest(packet: Packet): Unit = {}
}
