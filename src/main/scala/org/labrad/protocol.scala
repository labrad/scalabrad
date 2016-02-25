package org.labrad

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.handler.codec._
import io.netty.util.AttributeKey
import java.io.ByteArrayOutputStream
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

  // Byte order of this channel, based on first word of first received packet
  private var byteOrder: ByteOrder = forceByteOrder

  // Decoding state of the current packet.
  //
  // Previously we used ByteToMessageCodec's buffer management to accumulate
  // bytes until we had the full packet, but it has quadratic scaling and so
  // gets very slow for large packets. Instead, we use a ByteArrayOutputStream
  // to collect the bytes.
  case class DecodeState(context: Context, request: Int, target: Long, dataLen: Int, bytes: ByteArrayOutputStream)
  private var state: DecodeState = _

  protected override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: JList[AnyRef]): Unit = {
    // detect endianness from target field of first packet
    if (byteOrder == null) {
      if (in.readableBytes < 4) return

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

    if (state == null) {
      // Wait until the full header is available
      if (in.readableBytes < 20) return

      log.trace(s"decoding packet: ${in.order}")
      log.trace(s"buffer byteOrder: ${buf.order}")

      // Unpack the header
      val high = buf.readUnsignedInt
      val low = buf.readUnsignedInt
      val request = buf.readInt
      val target = buf.readUnsignedInt
      val dataLen = buf.readInt

      // Start decoding a new packet
      state = DecodeState(Context(high, low), request, target, dataLen, new ByteArrayOutputStream)
    }

    if (state != null) {
      val remaining = state.dataLen - state.bytes.size
      val available = Math.min(remaining, buf.readableBytes)
      buf.readBytes(state.bytes, available)

      if (state.bytes.size == state.dataLen) {
        // Unpack the received data into a list of records
        val bytes = state.bytes.toByteArray
        val recordBuf = Unpooled.wrappedBuffer(bytes).order(byteOrder)
        recordBuf.retain()
        val records = Packet.extractRecords(recordBuf)
        recordBuf.release()

        // Pass along the assembled packet
        val packet = Packet(state.request, state.target, state.context, records)
        out.add(packet)

        // Reset decoder state for next packet
        state = null
      }
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

