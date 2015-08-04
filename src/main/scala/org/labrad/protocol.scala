package org.labrad

import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.handler.codec._
import io.netty.util.AttributeKey
import java.nio.ByteOrder
import java.nio.ByteOrder.{BIG_ENDIAN, LITTLE_ENDIAN}
import java.util.{List => JList}
import org.labrad.data._
import org.labrad.types._
import org.labrad.util.Logging


/**
 * Decoder that reads incoming LabRAD packets on a netty channel.
 *
 * If forceByteOrder is null, the byte order will be detected from the first incoming
 * packet, suitable for use by the manager. For client or server connections
 * which send before they receive, forceByteOrder should instead be set to the
 * desired byte order for the connection.
 */
class PacketCodec(forceByteOrder: ByteOrder = null) extends ByteToMessageCodec[Packet] with Logging {

  private var byteOrder: ByteOrder = forceByteOrder

  protected override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: JList[AnyRef]): Unit = {
    // Wait until the full header is available
    if (in.readableBytes < 20) return

    // detect endianness from target field of first packet
    if (byteOrder == null) {
      byteOrder = in.getInt(12) match {
        case 0x00000001 => // endianness is okay. do nothing
          in.order

        case 0x01000000 => // endianness needs to be reversed
          in.order match {
            case BIG_ENDIAN => LITTLE_ENDIAN
            case LITTLE_ENDIAN => BIG_ENDIAN
          }

        case target =>
          sys.error(s"Invalid login packet. Expected target = 1 but got $target")
      }
    }
    val buf = in.order(byteOrder)

    log.trace(s"decoding packet: ${in.order}")
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
    } else {
      // Unpack the received data into a list of records
      val recordBuf = buf.readSlice(dataLen)
      val records = Packet.extractRecords(recordBuf)

      // Pass along the assembled packet
      val packet = Packet(request, target, Context(high, low), records)
      out.add(packet)
    }
  }

  override def encode(ctx: ChannelHandlerContext, packet: Packet, out: ByteBuf): Unit = {
    log.trace(s"write packet: $packet")
    val buf = out.order(byteOrder)
    packet.writeTo(buf)
  }
}


/**
 * Transforms context in incoming and outgoing packets:
 * - incoming: high context 0 replaced with the connection id
 * - outgoing: high context equal to connection id replaced with 0
 */
class ContextCodec(id: Long) extends MessageToMessageCodec[Packet, Packet] {

  override def decode(ctx: ChannelHandlerContext, msg: Packet, out: JList[AnyRef]): Unit = {
    val newMsg = msg match {
      case packet @ Packet(_, _, Context(0, low), _) =>
        packet.copy(context = Context(id, low))
      case msg => msg
    }
    out.add(newMsg)
  }

  override def encode(ctx: ChannelHandlerContext, msg: Packet, out: JList[AnyRef]): Unit = {
    val newMsg = msg match {
      case packet @ Packet(_, _, Context(`id`, low), _) =>
        packet.copy(context = Context(0, low))
      case msg => msg
    }
    out.add(newMsg)
  }
}

