package org.labrad

import java.io.ByteArrayInputStream
import java.nio.ByteOrder

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers, HeapChannelBufferFactory}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.handler.codec.oneone.{OneToOneDecoder, OneToOneEncoder}

import grizzled.slf4j.Logging

import data._
import types.Type


/** Determines the byte order of a byte stream by examining the header of the first packet sent.
 * 
 * The target id of the first packet should be 1, which will be encoded as 0x00000001 in
 * big-endian byte order or 0x01000000 in little-endian byte order.  The default byte order
 * for most network protocols is big-endian, and this is the recommended endianness.  If
 * little-endian byte order is detected, we create a new buffer factory for this channel
 * so the byte order will be properly handled.
 */
@ChannelHandler.Sharable
class ByteOrderDecoder extends FrameDecoder with Logging {
  
  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): AnyRef = {
    // Wait until full header is available
    if (buffer.readableBytes < 20) return null
    
    // Unpack the header
    buffer.markReaderIndex
    val ctxHigh = buffer.readInt
    val ctxLow = buffer.readInt
    val request = buffer.readInt
    val target = buffer.readInt
    val dataLen = buffer.readInt
    buffer.resetReaderIndex
    
    val newBuffer = target match {
      case 0x00000001 => // endianness is okay. do nothing
        debug("standard byte order")
        buffer
      case 0x01000000 => // endianness needs to be reversed
        val byteOrder = channel.getConfig.getBufferFactory.getDefaultOrder match {
          case ByteOrder.BIG_ENDIAN => ByteOrder.LITTLE_ENDIAN
          case ByteOrder.LITTLE_ENDIAN => ByteOrder.BIG_ENDIAN
        }
        debug("swapping byte order: " + byteOrder)
        channel.getConfig.setBufferFactory(new HeapChannelBufferFactory(byteOrder))
        val newBuffer = ChannelBuffers.wrappedBuffer(buffer.toByteBuffer.order(byteOrder))
        buffer.readBytes(buffer.readableBytes)
        newBuffer
      case _ =>
        throw new Exception("Invalid login packet")
    }
    
    ctx.getPipeline.remove(this)
    
    // hand off data to packet decoder
    newBuffer.readBytes(newBuffer.readableBytes)
  }
}


/** Decodes incoming bytes into LabRAD packets */
@ChannelHandler.Sharable
class PacketDecoder extends FrameDecoder with Logging {
  protected override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): AnyRef = {
    // Wait until the header is available.
    if (buffer.readableBytes < 20) return null
    
    debug("decoding packet: " + channel.getConfig.getBufferFactory.getDefaultOrder)
    debug("buffer byteOrder: " + buffer.order)
    
    // Unpack the header
    buffer.markReaderIndex
    val high = buffer.readUnsignedInt
    val low = buffer.readUnsignedInt
    val request = buffer.readInt
    val source = buffer.readUnsignedInt
    val dataLen = buffer.readInt
    
    // Wait until enough data is available
    if (buffer.readableBytes < dataLen) {
      buffer.resetReaderIndex
      return null
    }
    
    // Unpack the received data into a list of records
    val decoded = Array.ofDim[Byte](dataLen)
    buffer.readBytes(decoded)
    val stream = new ByteArrayInputStream(decoded)
    
    implicit val byteOrder = buffer.order
    val records = Seq.newBuilder[Record]
    while (stream.available > 0) {
      val recData = Data.fromBytes(stream, Type.RECORD)
      val Cluster(Word(id), Str(tag), Bytes(data)) = recData
      records += Record(id, Data.fromBytes(data, Type(tag)))
    }
    Packet(request, source, Context(high, low), records.result)
  }
}


/** Flattens outgoing LabRAD packets into a stream of bytes */
@ChannelHandler.Sharable
class PacketEncoder extends OneToOneEncoder with Logging {
  protected override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef =
    msg match {
      case Packet(request, source, context, records) =>
        val factory = channel.getConfig.getBufferFactory
        implicit val byteOrder = factory.getDefaultOrder
        
        debug("write packet: " + msg)
        val flatRecs = records.toArray.flatMap {
          case Record(id, data) =>
            Cluster(Word(id), Str(data.tag), Bytes(data.toBytes)).toBytes
        }
        
        val data = Cluster(context.toData, Integer(request), Word(source), Bytes(flatRecs))
        val bytes = data.toBytes
        val buf = ChannelBuffers.dynamicBuffer(factory)
        buf.writeBytes(bytes)
        buf
    }
}


/** Decoder for incoming packets that replaces high context 0 with the connection id */
class ContextDecoder(id: Long) extends OneToOneDecoder {
  override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = msg match {
    case packet @ Packet(_, _, Context(0, low), _) =>
      packet.copy(context = Context(id, 0))
    case msg => msg
  }
}


/** Encoder for outgoing packets that sets the high context to 0 if it is equal to the connection id */
class ContextEncoder(id: Long) extends OneToOneEncoder {
  override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = msg match {
    case packet @ Packet(_, _, Context(`id`, low), _) =>
      packet.copy(context = Context(0, low))
    case msg => msg
  }
}


