package org.labrad

import java.nio.ByteOrder
import java.nio.ByteOrder.{BIG_ENDIAN, LITTLE_ENDIAN}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers, HeapChannelBufferFactory}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.handler.codec.oneone.{OneToOneDecoder, OneToOneEncoder}
import org.labrad.data._
import org.labrad.types._
import org.labrad.util.Logging


/**
 * Determines the byte order of a byte stream by examining the header of the first packet sent.
 *
 * The target id of the first packet should be 1, which will be encoded as 0x00000001 in
 * big-endian byte order or 0x01000000 in little-endian byte order.  The default byte order
 * for most network protocols is big-endian, and this is the recommended endianness.  If
 * little-endian byte order is detected, we create a new buffer factory for this channel
 * so the byte order will be properly handled.
 */
object ByteOrderDecoder extends FrameDecoder with Logging {

  override def decode(ctx: ChannelHandlerContext, channel: Channel, buf: ChannelBuffer): AnyRef = {
    // Wait until full header is available
    if (buf.readableBytes < 20) return null

    // Unpack the header
    buf.markReaderIndex
    val ctxHigh = buf.readInt
    val ctxLow = buf.readInt
    val request = buf.readInt
    val target = buf.readInt
    val dataLen = buf.readInt
    buf.resetReaderIndex

    val newBuffer = target match {
      case 0x00000001 => // endianness is okay. do nothing
        log.debug("standard byte order")
        buf
      case 0x01000000 => // endianness needs to be reversed
        val byteOrder = buf.order match {
          case BIG_ENDIAN => LITTLE_ENDIAN
          case LITTLE_ENDIAN => BIG_ENDIAN
        }
        log.debug(s"swapped byte order: ${byteOrder}")
        channel.getConfig.setBufferFactory(new HeapChannelBufferFactory(byteOrder))
        val newBuffer = ChannelBuffers.wrappedBuffer(buf.toByteBuffer.order(byteOrder))
        buf.readBytes(buf.readableBytes)
        newBuffer
      case _ =>
        sys.error("Invalid login packet")
    }

    // having determined the byte order, our work here is done
    ctx.getPipeline.remove(this)

    // hand off data to packet decoder
    newBuffer.readBytes(newBuffer.readableBytes)
  }
}


/**
 * Decoder that reads incoming LabRAD packets on a netty channel
 */
object PacketDecoder extends FrameDecoder with Logging {
  protected override def decode(ctx: ChannelHandlerContext, ch: Channel, buf: ChannelBuffer): AnyRef = {
    // Wait until the full header is available
    if (buf.readableBytes < 20) return null

    log.trace(s"decoding packet: ${ch.getConfig.getBufferFactory.getDefaultOrder}")
    log.trace(s"buffer byteOrder: ${buf.order}")

    // Unpack the header
    buf.markReaderIndex
    val high = buf.readUnsignedInt
    val low = buf.readUnsignedInt
    val request = buf.readInt
    val target = buf.readUnsignedInt
    val dataLen = buf.readInt

    if (buf.readableBytes < dataLen) {
      // Wait until enough data is available
      buf.resetReaderIndex
      null
    } else {
      // Unpack the received data into a list of records
      val data = Array.ofDim[Byte](dataLen)
      buf.readBytes(data)
      val records = Packet.extractRecords(data)(buf.order)

      // Pass along the assembled packet
      Packet(request, target, Context(high, low), records)
    }
  }
}


/**
 * Encoder that outputs LabRAD packets on a netty channel
 */
object PacketEncoder extends OneToOneEncoder with Logging {
  override def encode(ctx: ChannelHandlerContext, ch: Channel, msg: AnyRef): AnyRef =
    msg match {
      case packet: Packet =>
        val factory = ch.getConfig.getBufferFactory
        implicit val byteOrder = factory.getDefaultOrder

        log.trace(s"write packet: $msg")
        val bytes = packet.toBytes
        val buf = ChannelBuffers.dynamicBuffer(factory)
        buf.writeBytes(bytes)
        buf
    }
}


/**
 * Transforms incoming packets by replacing high context 0 with the connection id
 */
class ContextDecoder(id: Long) extends OneToOneDecoder {
  override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = msg match {
    case packet @ Packet(_, _, Context(0, low), _) =>
      packet.copy(context = Context(id, low))
    case msg => msg
  }
}


/**
 * Transforms outgoing packets by setting high context to 0 if it is equal to the connection id
 */
class ContextEncoder(id: Long) extends OneToOneEncoder {
  override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = msg match {
    case packet @ Packet(_, _, Context(`id`, low), _) =>
      packet.copy(context = Context(0, low))
    case msg => msg
  }
}


