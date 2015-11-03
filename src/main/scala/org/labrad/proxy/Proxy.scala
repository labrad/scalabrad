package org.labrad.proxy

import io.netty.bootstrap.{Bootstrap, ServerBootstrap}
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.{NioSocketChannel, NioServerSocketChannel}
import org.clapper.argot._
import org.clapper.argot.ArgotConverters._
import org.labrad.Connection
import org.labrad.util.{ArgParsing, Logging, NettyUtil}
import org.labrad.util.Paths._
import scala.util.{Failure, Success, Try}

/**
 * Server that accepts incoming connections from localhost and
 * forwards them to a remote manager with TLS.
 */
class ProxyServer(port: Int, remoteHost: String, remotePort: Int) extends Logging {

  val sslContext = Connection.makeSslContext(remoteHost)

  val bossGroup = new NioEventLoopGroup(1)
  val workerGroup = new NioEventLoopGroup()

  /**
   * Connect to the remote manager and create a proxy channel tied
   * to the given local channel.
   */
  def connectRemote(local: Channel): Channel = {
    val b = new Bootstrap()
    b.group(workerGroup)
     .channel(classOf[NioSocketChannel])
     .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
     .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
     .handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(remote: SocketChannel): Unit = {
          val p = remote.pipeline
          p.addLast("sslHandler", sslContext.newHandler(remote.alloc(), remoteHost, remotePort))
          p.addLast("remoteHandler", new ProxyHandler(local))
        }
      })
    val remote = b.connect(remoteHost, remotePort).sync().channel
    log.info(s"connected to $remoteHost:$remotePort. channel=$remote")
    remote
  }

  val server = try {
    val b = new ServerBootstrap()
    b.group(bossGroup, workerGroup)
     .channel(classOf[NioServerSocketChannel])
     .childOption[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
     .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
     .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(local: SocketChannel): Unit = {
          if (!NettyUtil.isLocalConnection(local)) {
            log.error(s"proxy only accepts connections from localhost! remoteAddress=${local.remoteAddress}")
            local.close()
          } else {
            val remote = connectRemote(local)
            local.pipeline.addLast("localHandler", new ProxyHandler(remote))
          }
        }
      })
    val server = b.bind(port).sync().channel
    log.info(s"now accepting labrad connections: port=$port")
    server
  } catch {
    case e: Exception =>
      stop()
      throw e
  }

  def stop() {
    log.info("shutting down")
    try {
      server.close()
      server.closeFuture.sync()
    } finally {
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}

/**
 * Handler that forwards any incoming data to another channel.
 * If our channel is closed or has an error, closes the other channel as well.
 */
class ProxyHandler(other: Channel)
extends ChannelInboundHandlerAdapter with Logging {

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    other.writeAndFlush(msg)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    ctx.close()
    other.close()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, ex: Throwable): Unit = {
    log.error("exceptionCaught", ex)
    ctx.close()
    other.close()
  }
}

/**
 * Accept local unencrypted connections and forward them to a remote manager with TLS.
 */
object Proxy extends Logging {
  def main(args: Array[String]) {
    val config = ProxyConfig.fromCommandLine(args) match {
      case Success(config) => config

      case Failure(e: ArgotUsageException) =>
        println(e.message)
        return

      case Failure(e: Throwable) =>
        println(s"unexpected error: $e")
        return
    }
    val proxy = new ProxyServer(config.port, config.remoteHost, config.remotePort)
    scala.sys.addShutdownHook {
      proxy.stop()
    }
  }
}

/**
 * Configuration for running the labrad proxy.
 */
case class ProxyConfig(
  port: Int,
  remoteHost: String,
  remotePort: Int
)

object ProxyConfig {

  /**
   * Create ProxyConfig from command line and map of environment variables.
   *
   * @param args command line parameters
   * @param env map of environment variables, which defaults to the actual
   *        environment variables in scala.sys.env
   * @return a Try containing a ProxyConfig instance (on success) or a Failure
   *         in the case something went wrong. The Failure will contain an
   *         ArgotUsageException if the command line parsing failed or the
   *         -h or --help options were supplied.
   */
  def fromCommandLine(
    args: Array[String],
    env: Map[String, String] = scala.sys.env
  ): Try[ProxyConfig] = {
    val parser = new ArgotParser("labrad-proxy",
      preUsage = Some("Transparent labrad proxy."),
      sortUsage = false
    )
    val portOpt = parser.option[Int](
      names = List("port"),
      valueName = "int",
      description = "Port on which to listen for incoming connections. " +
        "If not provided, fallback to the value given in the LABRADPORT " +
        "environment variable, with default value 7682."
    )
    val remoteManagerOpt = parser.option[String](
      names = List("remote-manager"),
      valueName = "host[:port]",
      description = "The host and optional port of the remote manager to " +
        "which labrad connections will be proxied."
    )
    val help = parser.flag[Boolean](List("h", "help"),
      "Print usage information and exit")

    Try {
      parser.parse(ArgParsing.expandLongArgs(args))
      if (help.value.getOrElse(false)) parser.usage()

      val port = portOpt.value.orElse(env.get("LABRADPORT").map(_.toInt)).getOrElse(7682)
      val (remoteManager, remotePort) = remoteManagerOpt.value.get.split(":") match {
        case Array(host) => (host, 7643)
        case Array(host, port) => (host, port.toInt)
      }

      ProxyConfig(
        port,
        remoteManager,
        remotePort)
    }
  }

}
