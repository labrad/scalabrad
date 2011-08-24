package org.labrad
package data

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory

import util._

class TestClientHandler extends SimpleChannelHandler {
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    val ch = e.getChannel
    
    val f = ch.write(Packet(1, 1, Context(0, 1), Seq()))
    f.addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture) {
        println("finished writing packet...")
      }
    })
  }
  
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case packet: Packet => println("package received: " + packet)
    }
  }
  
  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    e.getCause.printStackTrace
    e.getChannel.close
  }
}

object TestClientNetty {
  def main(args: Array[String]) {
    val host = Util.getEnv("LABRADHOST", Constants.DEFAULT_HOST) //"10.5.5.128"; //args[0];
    val port = 7682; //Integer.parseInt(args[1]);

    val factory =
        new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool,
                Executors.newCachedThreadPool)

    val bootstrap = new ClientBootstrap(factory)

    bootstrap.getPipeline.addLast("encoder", new PacketEncoder)
    bootstrap.getPipeline.addLast("decoder", new PacketDecoder)
    bootstrap.getPipeline.addLast("handler", new TestClientHandler)
    
    bootstrap.setOption("tcpNoDelay", true)
    bootstrap.setOption("keepAlive", true)

    val f = bootstrap.connect(new InetSocketAddress(host, port))
    f.awaitUninterruptibly
    
    if (!f.isSuccess) f.getCause.printStackTrace
    f.getChannel.getCloseFuture.awaitUninterruptibly
    
    factory.releaseExternalResources
  }
}
