package org.labrad.manager

import io.netty.channel._
import io.netty.handler.ssl.{SslContext, SslHandler}
import io.netty.util.DomainNameMapping
import java.io.{ByteArrayOutputStream, File, FileInputStream}
import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import org.labrad.ContextCodec
import org.labrad.data._
import org.labrad.errors._
import org.labrad.types._
import org.labrad.util._
import scala.concurrent.ExecutionContext
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
    val certMapping = new DomainNameMapping[String](readFile(defaultCertFile))
    val certFileMapping = new DomainNameMapping[File](defaultCertFile)
    val ctxMapping = new DomainNameMapping[SslContext](defaultSslCtx)

    for ((host, (certFile, sslCtx)) <- hosts) {
      certMapping.add(host, readFile(certFile))
      certFileMapping.add(host, certFile)
      ctxMapping.add(host, sslCtx)
    }

    TlsHostConfig(certMapping, certFileMapping, ctxMapping)
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
)(implicit ec: ExecutionContext)
extends SimpleChannelInboundHandler[Packet] with Logging {

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    isLocalConnection = (ctx.channel.remoteAddress, ctx.channel.localAddress) match {
      case (remote: InetSocketAddress, local: InetSocketAddress) =>
        remote.getAddress.isLoopbackAddress || remote.getAddress == local.getAddress

      case _ =>
        false
    }
    log.info(s"remote=${ctx.channel.remoteAddress}, local=${ctx.channel.localAddress}, isLocalConnection=$isLocalConnection")
  }

  override def channelRead0(ctx: ChannelHandlerContext, packet: Packet): Unit = {
    val Packet(req, target, context, records) = packet
    val resp = try {
      handle(ctx, packet)
    } catch {
      case ex: LabradException =>
        log.debug("error during login", ex)
        ex.toData
      case ex: Throwable =>
        log.debug("error during login", ex)
        Error(1, ex.toString)
    }
    val future = ctx.channel.writeAndFlush(Packet(-req, target, context, Seq(Record(0, resp))))
    if (resp.isError) future.addListener(ChannelFutureListener.CLOSE)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, ex: Throwable): Unit = {
    log.error("exceptionCaught", ex)
    ctx.close()
  }

  // whether the connection originated from localhost (if so, we allow unencrypted connections)
  private var isLocalConnection: Boolean = false

  // whether the connection is secure (either because this is a tls-only channel
  // or we have upgraded with STARTTLS to a secure connection.
  private var isSecure: Boolean = tlsPolicy == TlsPolicy.ON

  private val challenge = Array.ofDim[Byte](256)
  Random.nextBytes(challenge)

  private var handle: (ChannelHandlerContext, Packet) => Data = {
    import TlsPolicy._
    tlsPolicy match {
      case STARTTLS | STARTTLS_OPT | STARTTLS_FORCE => handleStartTls
      case ON | OFF => handleLogin
    }
  }

  private def handleStartTls(ctx: ChannelHandlerContext, packet: Packet): Data = packet match {
    case Packet(req, 1, _, Seq(Record(1, Cluster(Str("STARTTLS"), Str(host))))) =>
      val sslContext = tlsHostConfig.sslCtxs.map(host)
      val engine = sslContext.newEngine(ctx.alloc())
      val sslHandler = new SslHandler(engine, true)
      ctx.pipeline.addFirst(sslHandler)
      isSecure = true // now upgraded to TLS
      handle = handleLogin
      Str(tlsHostConfig.certs.map(host))

    case _ =>
      val requireStartTls = tlsPolicy match {
        case TlsPolicy.STARTTLS_FORCE => true
        case TlsPolicy.STARTTLS if !isLocalConnection => true
        case _ => false
      }
      if (requireStartTls) {
        throw LabradException(2, "Expected STARTTLS")
      }
      handleLogin(ctx, packet)
  }

  private def handleLogin(ctx: ChannelHandlerContext, packet: Packet): Data = packet match {
    case Packet(req, 1, _, Seq()) if req > 0 =>
      handle = handleChallengeResponse
      Bytes(challenge)

    case Packet(req, 1, _, Seq(Record(2, Str("PING")))) =>
      Str("PONG")

    case _ =>
      throw LabradException(1, "Invalid login packet")
  }

  private def handleChallengeResponse(ctx: ChannelHandlerContext, packet: Packet): Data = packet match {
    case Packet(req, 1, _, Seq(Record(0, Bytes(response)))) if req > 0 =>
      if (!auth.authenticate(challenge, response)) throw LabradException(2, "Incorrect password")
      handle = handleIdentification
      Str("LabRAD 2.0")
    case _ =>
      throw LabradException(1, "Invalid authentication packet")
  }

  private def handleIdentification(ctx: ChannelHandlerContext, packet: Packet): Data = packet match {
    case Packet(req, 1, _, Seq(Record(0, data))) if req > 0 =>
      val (handler, id) = data match {
        case Cluster(UInt(ver), Str(name)) =>
          val id = hub.allocateClientId(name)
          val handler = new ClientHandler(hub, tracker, messager, ctx.channel, id, name)
          hub.connectClient(id, name, handler)
          (handler, id)

        case Cluster(UInt(ver), Str(name), Str(doc)) =>
          val id = hub.allocateServerId(name)
          val handler = new ServerHandler(hub, tracker, messager, ctx.channel, id, name, doc)
          hub.connectServer(id, name, handler)
          (handler, id)

        // TODO: remove this case (collapse doc and notes)
        case Cluster(UInt(ver), Str(name), Str(docOrig), Str(notes)) =>
          val doc = if (notes.isEmpty) docOrig else (docOrig + "\n\n" + notes)
          val id = hub.allocateServerId(name)
          val handler = new ServerHandler(hub, tracker, messager, ctx.channel, id, name, doc)
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

      UInt(id)

    case _ =>
      throw LabradException(1, "Invalid identification packet")
  }
}
