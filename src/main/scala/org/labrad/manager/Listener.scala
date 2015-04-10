package org.labrad.manager

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import org.labrad.PacketCodec
import org.labrad.util._
import scala.concurrent.ExecutionContext

/**
 * listens for incoming labrad network connections
 */
class Listener(auth: AuthService, hub: Hub, tracker: StatsTracker, messager: Messager, port: Int)(implicit ec: ExecutionContext)
extends Logging {

  val bossGroup = new NioEventLoopGroup(1)
  val workerGroup = new NioEventLoopGroup()

  val serverChannel = try {
    val b = new ServerBootstrap()
    b.group(bossGroup, workerGroup)
     .channel(classOf[NioServerSocketChannel])
     .childOption[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
     .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
     .childHandler(new ChannelInitializer[SocketChannel] {
         override def initChannel(ch: SocketChannel): Unit = {
           val p = ch.pipeline
           p.addLast("packetCodec", new PacketCodec)
           p.addLast("loginHandler", new LoginHandler(auth, hub, tracker, messager))
         }
     })

    // Bind and start to accept incoming connections.
    b.bind(port).sync().channel
  } catch {
    case e: Exception =>
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
      throw e
  }

  log.info(s"now accepting labrad connections on port $port")

  def stop() {
    log.info("shutting down")
    try {
      serverChannel.close()
      serverChannel.closeFuture.sync()
    } finally {
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}
