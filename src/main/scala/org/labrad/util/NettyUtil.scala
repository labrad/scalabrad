package org.labrad.util

import java.net.InetSocketAddress
import io.netty.channel.Channel

object NettyUtil {

  /**
   * Determine whether the given SocketChannel represents a connection from the local machine.
   */
  def isLocalConnection(ch: Channel): Boolean = {
    (ch.remoteAddress, ch.localAddress) match {
      case (remote: InetSocketAddress, local: InetSocketAddress) =>
        remote.getAddress.isLoopbackAddress || remote.getAddress == local.getAddress

      case _ =>
        false
    }
  }

}
