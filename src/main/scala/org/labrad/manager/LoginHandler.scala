package org.labrad.manager

import io.netty.channel._
import io.netty.handler.ssl.{SslContext, SslHandler}
import io.netty.util.{DomainNameMapping, DomainNameMappingBuilder}
import java.io.{ByteArrayOutputStream, File, FileInputStream}
import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.concurrent.{ScheduledExecutorService, TimeoutException}
import org.labrad.ContextCodec
import org.labrad.concurrent.Chan
import org.labrad.concurrent.Go._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.manager.auth._
import org.labrad.types._
import org.labrad.util._
import org.labrad.util.Futures._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.Random

/**
 * Configuration of TLS cert/key pairs for one or more hostnames.
 *
 * This is used with SNI (Server Name Indication) to allow the manager to
 * present the appropriate certificate to the client, based on the hostname
 * to which the client is connecting. We also need this for STARTTLS, where
 * the client sends the hostname along with the STARTTLS request so that we
 * can pick the correct cert.
 */
case class TlsHostConfig(
  certs: DomainNameMapping[String],
  certFiles: DomainNameMapping[File],
  sslCtxs: DomainNameMapping[SslContext]
)

object TlsHostConfig {
  def apply(default: (File, SslContext), hosts: (String, (File, SslContext))*): TlsHostConfig = {
    val (defaultCertFile, defaultSslCtx) = default
    val certMapping = new DomainNameMappingBuilder[String](readFile(defaultCertFile))
    val certFileMapping = new DomainNameMappingBuilder[File](defaultCertFile)
    val ctxMapping = new DomainNameMappingBuilder[SslContext](defaultSslCtx)

    for ((host, (certFile, sslCtx)) <- hosts) {
      certMapping.add(host, readFile(certFile))
      certFileMapping.add(host, certFile)
      ctxMapping.add(host, sslCtx)
    }

    TlsHostConfig(
      certs = certMapping.build(),
      certFiles = certFileMapping.build(),
      sslCtxs = ctxMapping.build()
    )
  }

  private def readFile(f: File): String = new String(Files.readAllBytes(f.toPath), UTF_8)
}

/**
 * Channel handler for the initial login messages of a labrad connection.
 *
 * At the end of a successful login, this handler removes itself from the
 * channel pipeline and instead adds an appropriate client or server handler
 * to handle subsequent data, depending on the connection type.
 */
class LoginHandler(
  auth: AuthService,
  hub: Hub,
  tracker: StatsTracker,
  messager: Messager,
  tlsHostConfig: TlsHostConfig,
  tlsPolicy: TlsPolicy
)(implicit ec: ExecutionContext, scheduler: ScheduledExecutorService)
extends SimpleChannelInboundHandler[Packet] with Logging {

  // whether the connection originated from localhost (if so, we allow unencrypted connections)
  private var isLocalConnection: Boolean = false

  // whether the connection is secure (either because this is a tls-only channel
  // or we have upgraded with STARTTLS to a secure connection.
  private var isSecure: Boolean = tlsPolicy == TlsPolicy.ON

  private var username: String = ""

  import TlsPolicy._
  private val canStartTls = tlsPolicy match {
    case STARTTLS | STARTTLS_OPT | STARTTLS_FORCE => true
    case ON | OFF => false
  }

  case class Req(packet: Packet, promise: Promise[Data])
  private val reqs = Chan[Req](1)

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    this.ctx = ctx
    isLocalConnection = (ctx.channel.remoteAddress, ctx.channel.localAddress) match {
      case (remote: InetSocketAddress, local: InetSocketAddress) =>
        remote.getAddress.isLoopbackAddress || remote.getAddress == local.getAddress

      case _ =>
        false
    }
    log.info(s"remote=${ctx.channel.remoteAddress}, local=${ctx.channel.localAddress}, isLocalConnection=$isLocalConnection")
    doLogin()
  }

  private val lock = new AsyncSemaphore(1)
  private var ctx: ChannelHandlerContext = null

  override def channelRead0(ctx: ChannelHandlerContext, packet: Packet): Unit = {
    val Packet(req, target, context, records) = packet
    val promise = Promise[Data]
    reqs.send(Req(packet, promise))
    promise.future.recover {
      case ex: LabradException =>
        log.debug("error during login", ex)
        ex.toData
      case ex: Throwable =>
        log.debug("error during login", ex)
        Error(1, ex.toString)
    }.map { resp =>
      val future = ctx.channel.writeAndFlush(Packet(-req, target, context, Seq(Record(0, resp))))
      if (resp.isError) future.addListener(ChannelFutureListener.CLOSE)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, ex: Throwable): Unit = {
    log.error("exceptionCaught", ex)
    ctx.close()
  }

  private def reply(packet: Packet, resp: Data): Unit = {
    val Packet(req, target, context, records) = packet
    val future = ctx.channel.writeAndFlush(Packet(-req, target, context, Seq(Record(0, resp))))
    if (resp.isError) future.addListener(ChannelFutureListener.CLOSE)
  }

  // TODO: need to capture overall goroutine errors, send them back and close the connection

  private def doLogin(): Future[Unit] = go {
    var request: Req = null
    try {
      if (canStartTls) {
        request = reqs.recv()
        request.packet match {
          case Packet(req, 1, _, Seq(Record(1, Cluster(Str("STARTTLS"), Str(host))))) =>
            val sslContext = tlsHostConfig.sslCtxs.map(host)
            val engine = sslContext.newEngine(ctx.alloc())
            val sslHandler = new SslHandler(engine, true)
            ctx.pipeline.addFirst(sslHandler)
            isSecure = true // now upgraded to TLS
            request.promise.success(Str(tlsHostConfig.certs.map(host)))
            request = reqs.recv()

          case _ =>
            val requireStartTls = tlsPolicy match {
              case TlsPolicy.STARTTLS_FORCE => true
              case TlsPolicy.STARTTLS if !isLocalConnection => true
              case _ => false
            }
            if (requireStartTls) {
              throw LabradException(2, "Expected STARTTLS")
            }
        }
      }

      var loggedIn = false
      while (!loggedIn) {
        request.packet match {
          case Packet(req, 1, _, Seq()) if req > 0 =>
            val challenge = Array.ofDim[Byte](256)
            Random.nextBytes(challenge)
            // For compatibility, send 's' here, even though challenge is binary.
            //Bytes(challenge)
            val d = TreeData("s")
            d.setBytes(challenge)
            request.promise.success(d)
            request = reqs.recv()
            checkChallengeResponse(request.packet, challenge)
            request.promise.success(Str("LabRAD 2.0"))
            loggedIn = true

          case Packet(req, 1, _, Seq(Record(2, Str("PING")))) =>
            request.promise.success(("PONG", Seq("auth-server")).toData) // include manager features in ping response
            request = reqs.recv()

          case Packet(req, 1, _, Seq(Record(Authenticator.METHODS_SETTING_ID, data))) =>
            val resp = doAuthRequest(Authenticator.METHODS_SETTING_ID, data)
            request.promise.success((Seq("password") ++ resp.get[Seq[String]]).toData)
            request = reqs.recv()

          case Packet(req, 1, _, Seq(Record(Authenticator.INFO_SETTING_ID, data))) =>
            val resp = doAuthRequest(Authenticator.INFO_SETTING_ID, data)
            request.promise.success(resp)
            request = reqs.recv()

          case Packet(req, 1, _, Seq(Record(Authenticator.AUTH_SETTING_ID, data))) =>
            val resp = doAuthRequest(Authenticator.AUTH_SETTING_ID, data)
            username = resp.get[String]
            loggedIn = true
            request.promise.success(Str("LabRAD 2.0"))

          case _ =>
            throw LabradException(1, "Invalid login packet")
        }
      }

      request = reqs.recv()
      val id = handleIdentification(request.packet)
      request.promise.success(UInt(id))
    } catch {
      case ex: LabradException =>
        log.debug("error during login", ex)
        request.promise.success(ex.toData)
      case ex: Throwable =>
        log.debug("error during login", ex)
        request.promise.success(Error(1, ex.toString))
    }
  }

  private def doAuthRequest(setting: Long, data: Data): Data = {
    if (!(isSecure || isLocalConnection)) {
      throw LabradException(3, "External auth is only available with TLS")
    }
    try {
      hub.authServerConnected.withTimeout(5.seconds).recv
    } catch {
      case _: TimeoutException =>
        throw LabradException(4, "Timeout while waiting for auth server to connect")
    }
    val id = hub.getServerId(Authenticator.NAME)
    val packet = Packet(1, 1, Context(1, 0), Seq(Record(setting, data)))
    val respPacket = hub.request(id, packet)(timeout = 5.seconds).recv.get
    val Packet(_, _, _, Seq(Record(_, resp))) = respPacket
    if (resp.isError) {
      throw LabradException(resp.getErrorCode, resp.getErrorMessage)
    }
    resp
  }

  private def checkChallengeResponse(packet: Packet, challenge: Array[Byte]): Unit = packet match {
    case Packet(req, 1, _, Seq(Record(0, Bytes(response)))) if req > 0 =>
      if (!auth.authenticate(challenge, response)) throw LabradException(2, "Incorrect password")

    case _ =>
      throw LabradException(1, "Invalid authentication packet")
  }

  private def handleIdentification(packet: Packet): Long = packet match {
    case Packet(req, 1, _, Seq(Record(0, data))) if req > 0 =>
      val (handler, id) = data match {
        case Cluster(UInt(ver), Str(name)) =>
          val id = hub.allocateClientId(name)
          val handler = new ClientHandler(hub, tracker, messager, ctx.channel, id, name, username)
          hub.connectClient(id, name, handler)
          (handler, id)

        case Cluster(UInt(ver), Str(name), Str(doc)) =>
          val id = hub.allocateServerId(name)
          val handler = new ServerHandler(hub, tracker, messager, ctx.channel, id, name, doc, username)
          hub.connectServer(id, name, handler)
          (handler, id)

        // TODO: remove this case (collapse doc and notes)
        case Cluster(UInt(ver), Str(name), Str(docOrig), Str(notes)) =>
          val doc = if (notes.isEmpty) docOrig else (docOrig + "\n\n" + notes)
          val id = hub.allocateServerId(name)
          val handler = new ServerHandler(hub, tracker, messager, ctx.channel, id, name, doc, username)
          hub.connectServer(id, name, handler)
          (handler, id)

        case _ =>
          throw LabradException(1, "Invalid identification packet")
      }

      // logged in successfully; add new handler to channel pipeline
      val pipeline = ctx.pipeline
      pipeline.addLast("contextCodec", new ContextCodec(id))
      pipeline.addLast(handler.getClass.toString, handler)
      pipeline.remove(this)

      try {
        hub.registryConnected.withTimeout(30.seconds).recv.get
      } catch {
        case _: TimeoutException =>
          throw LabradException(3, "Timeout while waiting for registry to connect")
      }

      id

    case _ =>
      throw LabradException(1, "Invalid identification packet")
  }
}
