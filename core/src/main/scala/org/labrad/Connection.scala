package org.labrad

import io.netty.bootstrap.Bootstrap
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelOption, ChannelInitializer, EventLoopGroup, SimpleChannelInboundHandler}
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
import java.util.concurrent.{ExecutionException, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import org.labrad.Labrad.Manager
import org.labrad.Labrad.Authenticator
import org.labrad.data._
import org.labrad.errors._
import org.labrad.events.MessageListener
import org.labrad.util.{Counter, LookupProvider, NettyUtil}
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

object Connection {
  private val groupCounter = new AtomicLong(0)

  def newWorkerGroup(): EventLoopGroup =
    NettyUtil.newEventLoopGroup("LabradConnection", groupCounter)

  lazy val defaultWorkerGroup: EventLoopGroup = {
    val group = newWorkerGroup()
    sys.addShutdownHook {
      group.shutdownGracefully(10, 100, TimeUnit.MILLISECONDS).sync()
    }
    group
  }
}

sealed trait Credential
case class Password(username: String, password: Array[Char]) extends Credential
case class OAuthIdToken(idToken: String) extends Credential
case class OAuthAccessToken(accessToken: String) extends Credential

/**
 * A client or server connection to the Labrad manager.
 */
trait Connection {

  val name: String
  val host: String
  val port: Int
  val tls: TlsMode
  val tlsCerts: Map[String, File]
  val workerGroup: EventLoopGroup
  implicit val executionContext: ExecutionContext

  def credential: Credential

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
  protected val lookupProvider = new LookupProvider(this.send)
  protected val requestDispatcher = new RequestDispatcher(sendPacket)

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

  def connect(login: Boolean = true, timeout: Duration = 10.seconds): Unit = {
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
        val Str(cert) = Await.result(sendManagerRequest(1, ("STARTTLS", host).toData), timeout)

        var handler = makeSslContext(host).newHandler(channel.alloc(), host, port)
        channel.pipeline.addFirst("sslHandler", handler)
        Await.result(handler.handshakeFuture.toScala, 10.seconds)
      }

      if (login) {
        doLogin(credential, isLocalConnection || useStartTls || tls == TlsMode.ON, timeout)
      }
    } catch {
      case e: Throwable => close(e); throw e
    }

    for (listener <- connectionListeners) listener.lift(true)
  }

  private def doLogin(
    credential: Credential,
    isSecureOrLocal: Boolean,
    timeout: Duration = 10.seconds
  ): Unit = {
    try {
      val loginResponse = credential match {
        case Password("", password) =>
          // send first ping packet; response is password challenge
          val Bytes(challenge) = Await.result(sendManagerRequest(), timeout)

          val md = MessageDigest.getInstance("MD5")
          md.update(challenge)
          md.update(UTF_8.encode(CharBuffer.wrap(password)))
          val data = TreeData("s") // use s instead of y for backwards compatibility
          data.setBytes(md.digest)

          // send password response; response is welcome message
          try {
            Await.result(sendManagerRequest(0, data), timeout)
          } catch {
            case e: ExecutionException => throw new IncorrectPasswordException
          }

        case Password(username, password) =>
          require(isSecureOrLocal, "username+password requires secure connection")
          val data = ("username+password", (username, new String(password))).toData
          Await.result(sendManagerRequest(Authenticator.AUTH_SETTING_ID, data), timeout)

        case OAuthIdToken(idToken) =>
          require(isSecureOrLocal, "oauth_token requires secure connection")
          val data = ("oauth_token", idToken).toData
          Await.result(sendManagerRequest(Authenticator.AUTH_SETTING_ID, data), timeout)

        case OAuthAccessToken(accessToken) =>
          require(isSecureOrLocal, "oauth_access_token requires secure connection")
          val data = ("oauth_access_token", accessToken).toData
          Await.result(sendManagerRequest(Authenticator.AUTH_SETTING_ID, data), timeout)
      }

      loginMessage = loginResponse.get[String]

      // send identification packet; response is our assigned connection id
      val UInt(assignedId) = Await.result(sendManagerRequest(0, loginData), timeout)
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
  }

  private def closeNoWait(cause: Throwable): Unit = {
    val listeners = synchronized {
      if (connected) {
        connected = false
        requestDispatcher.failAll(cause)

        channel.close()
        connectionListeners.map(_.lift)
      } else {
        Seq()
      }
    }
    for (listener <- listeners) listener(false)
  }

  def sendManagerRequest(settingId: Long = -1, data: Data = Data.NONE): Future[Data] = {
    val records = settingId match {
      case -1 => Nil
      case settingId => Seq(Record(settingId, data))
    }
    send(Request(Manager.ID, records = records)).map { results => results(0) }
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
