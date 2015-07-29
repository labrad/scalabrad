package org.labrad.manager

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.ssl.{SniHandler, SslContext}
import io.netty.util.DomainNameMapping
import org.labrad.PacketCodec
import org.labrad.util._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/**
 * Manager policies for securing incoming client/server connections with TLS.
 */
sealed trait TlsPolicy

object TlsPolicy {
  /**
   * Initial connection uses TLS.
   */
  case object ON extends TlsPolicy

  /**
   * No TLS; connection is unencrypted and attempts to upgrade with STARTTLS
   * will be rejected. This is provided for testing and for compatibility with
   * the old manager.
   */
  case object OFF extends TlsPolicy

  /**
   * Default STARTTLS behavior. Connection starts unencrypted and we use
   * STARTTLS to upgrade later. STARTTLS is required for remote connections,
   * but optional for connections from localhost.
   */
  case object STARTTLS extends TlsPolicy

  /**
   * STARTTLS is optional for all connections, including from remote hosts.
   */
  case object STARTTLS_OPT extends TlsPolicy

  /**
   * STARTTLS is required for all connection, including from localhost.
   */
  case object STARTTLS_FORCE extends TlsPolicy

  def fromString(s: String): TlsPolicy = {
    s.toLowerCase match {
      case "on" => ON
      case "off" => OFF
      case "starttls" => STARTTLS
      case "starttls-opt" => STARTTLS_OPT
      case "starttls-force" => STARTTLS_FORCE
      case _ => sys.error(s"Invalid tls mode '$s'. Expected 'on', 'off', or 'starttls'.")
    }
  }
}

/**
 * Listens on one or more ports for incoming labrad network connections.
 */
class Listener(
  auth: AuthService,
  hub: Hub,
  tracker: StatsTracker,
  messager: Messager,
  listeners: Seq[(Int, TlsPolicy)],
  tlsHostConfig: TlsHostConfig
)(implicit ec: ExecutionContext)
extends Logging {
  val bossGroup = new NioEventLoopGroup(1)
  val workerGroup = new NioEventLoopGroup()

  def bootServer(port: Int, tlsPolicy: TlsPolicy): Channel = {
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
       .channel(classOf[NioServerSocketChannel])
       .childOption[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
       .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
       .childHandler(new ChannelInitializer[SocketChannel] {
         override def initChannel(ch: SocketChannel): Unit = {
           val p = ch.pipeline
           if (tlsPolicy == TlsPolicy.ON) {
             p.addLast(new SniHandler(tlsHostConfig.sslCtxs))
           }
           p.addLast("packetCodec", new PacketCodec())
           p.addLast("loginHandler",
             new LoginHandler(auth, hub, tracker, messager, tlsHostConfig, tlsPolicy))
         }
       })

      // Bind and start to accept incoming connections.
      val ch = b.bind(port).sync().channel
      log.info(s"now accepting labrad connections: port=$port, tlsPolicy=$tlsPolicy")
      ch
    } catch {
      case e: Exception =>
        stop()
        throw e
    }
  }

  val listenerTrys = listeners.map { case (port, tlsPolicy) => Try(bootServer(port, tlsPolicy)) }
  val channels = listenerTrys.collect { case Success(ch) => ch }
  val failures = listenerTrys.collect { case Failure(e) => e }

  // If any listeners failed to start, shutdown those that _did_ start and fail.
  if (!failures.isEmpty) {
    shutdown(channels)
    throw new Exception(s"some listeners failed to start: ${failures.mkString(", ")}")
  }

  def stop() {
    log.info("shutting down")
    shutdown(channels)
  }

  private def shutdown(listeners: Seq[Channel]): Unit = {
    try {
      for (ch <- listeners) {
        ch.close()
        ch.closeFuture.sync()
      }
    } finally {
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}
