package org.labrad.manager

import java.net.InetSocketAddress
import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers, HeapChannelBufferFactory}
import org.jboss.netty.channel._
import org.jboss.netty.channel.group._
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.labrad.{ByteOrderDecoder, PacketDecoder, PacketEncoder}
import org.labrad.util._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._

/**
 * listens for incoming labrad network connections
 */
class Listener(auth: AuthService, hub: Hub, tracker: StatsTracker, messager: Messager, port: Int)(implicit ec: ExecutionContext)
extends Logging {
  private val factory = new NioServerSocketChannelFactory(
    Executors.newCachedThreadPool,
    Executors.newCachedThreadPool
  )

  private val channels = new DefaultChannelGroup("labrad")
  private val groupHandler = new SimpleChannelHandler with Logging {
    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
      channels.add(e.getChannel)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
      log.error("disconnecting due to unhandled error", e.getCause)
      e.getChannel.close
    }
  }

  private val bootstrap = new ServerBootstrap(factory)
  bootstrap.setOption("child.tcpNoDelay", true)
  bootstrap.setOption("child.keepAlive", true)
  bootstrap.setPipelineFactory(new ChannelPipelineFactory {
    def getPipeline = {
      val pipeline = Channels.pipeline
      pipeline.addLast("groupHandler", groupHandler)
      pipeline.addLast("byteOrderDecoder", ByteOrderDecoder)
      pipeline.addLast("packetEncoder", PacketEncoder)
      pipeline.addLast("packetDecoder", PacketDecoder)
      pipeline.addLast("loginHandler", new LoginHandler(auth, hub, tracker, messager))
      pipeline
    }
  })

  private val server = bootstrap.bind(new InetSocketAddress(port))
  channels.add(server)

  log.info(s"now accepting labrad connections on port $port")

  def stop() {
    log.warn("shutting down")
    channels.close.awaitUninterruptibly
    factory.releaseExternalResources
  }
}
