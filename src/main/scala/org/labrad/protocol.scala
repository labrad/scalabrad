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


@ChannelHandler.Sharable
class PacketDecoder extends FrameDecoder {
  protected override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): AnyRef = {
    // Wait until the header is available.
    if (buffer.readableBytes < 20) return null
    
    //println("decoding packet: " + channel.getConfig.getBufferFactory.getDefaultOrder)
    //println("buffer byteOrder: " + buffer.order)
    
    // Unpack the header
    buffer.markReaderIndex
    val high = buffer.readUnsignedInt
    val low = buffer.readUnsignedInt
    val request = buffer.readInt
    val source = buffer.readUnsignedInt
    val dataLen = buffer.readInt
    
    // Wait until all data is available.
    if (buffer.readableBytes < dataLen) {
      buffer.resetReaderIndex
      return null
    }
    
    // Unpack the received data into a list of records.
    val decoded = Array.ofDim[Byte](dataLen)
    buffer.readBytes(decoded)
    val stream = new ByteArrayInputStream(decoded)
    
    val records = Seq.newBuilder[Record]
    while (stream.available > 0) {
      val recData = Data.fromBytes(stream, Type.RECORD)
      val Cluster(Word(id), Str(tag), Bytes(data)) = recData
      records += Record(id, Data.fromBytes(data, Type(tag)))
    }
    Packet(request, source, Context(high, low), records.result)
  }
}


@ChannelHandler.Sharable
class PacketEncoder extends OneToOneEncoder with Logging {
  protected override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef): AnyRef =
    msg match {
      case Packet(request, source, context, records) =>
        debug("write packet: " + msg)
        val flatRecs = records.flatMap {
          case Record(id, data) =>
            Cluster(Word(id), Str(data.tag), Bytes(data.toBytes)).toBytes
        }.toArray
        
        val data = Cluster(context.toData, Integer(request), Word(source), Bytes(flatRecs))
        val bytes = data.toBytes
        val buf = ChannelBuffers.dynamicBuffer
        buf.writeBytes(bytes)
        buf
    }
}


class ContextDecoder(id: Long) extends OneToOneDecoder {
  override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = msg match {
    case packet @ Packet(_, _, Context(0, low), _) =>
      packet.copy(context = Context(id, 0))
    case msg => msg
  }
}

class ContextEncoder(id: Long) extends OneToOneEncoder {
  override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = msg match {
    case packet @ Packet(_, _, Context(`id`, low), _) =>
      packet.copy(context = Context(0, low))
    case msg => msg
  }
}


