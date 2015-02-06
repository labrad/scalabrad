package org.labrad.manager

import io.netty.channel._
import org.labrad.ContextCodec
import org.labrad.data._
import org.labrad.errors._
import org.labrad.types._
import org.labrad.util._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

class LoginHandler(auth: AuthService, hub: Hub, tracker: StatsTracker, messager: Messager)(implicit ec: ExecutionContext)
extends SimpleChannelInboundHandler[Packet] with Logging {

  override def channelRead0(ctx: ChannelHandlerContext, packet: Packet): Unit = {
    val Packet(req, target, context, records) = packet
    val resp = try {
      handle(ctx, packet)
    } catch {
      case ex: LabradException =>
        log.debug("error during login", ex)
        ex.toData
      case ex: Throwable =>
        log.debug("error during login", ex)
        Error(1, ex.toString)
    }
    val future = ctx.channel.writeAndFlush(Packet(-req, target, context, Seq(Record(0, resp))))
    if (resp.isError) future.addListener(ChannelFutureListener.CLOSE)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, ex: Throwable): Unit = {
    log.error("exceptionCaught", ex)
    ctx.close()
  }

  private val challenge = Array.ofDim[Byte](256)
  Random.nextBytes(challenge)

  private var handle: (ChannelHandlerContext, Packet) => Data = handleLogin

  private def handleLogin(ctx: ChannelHandlerContext, packet: Packet): Data = packet match {
    case Packet(req, 1, _, Seq()) if req > 0 =>
      handle = handleChallengeResponse
      Bytes(challenge)
    case _ =>
      throw LabradException(1, "Invalid login packet")
  }

  private def handleChallengeResponse(ctx: ChannelHandlerContext, packet: Packet): Data = packet match {
    case Packet(req, 1, _, Seq(Record(0, Bytes(response)))) if req > 0 =>
      if (!auth.authenticate(challenge, response)) throw LabradException(2, "Incorrect password")
      handle = handleIdentification
      Str("LabRAD 2.0")
    case _ =>
      throw LabradException(1, "Invalid authentication packet")
  }

  private def handleIdentification(ctx: ChannelHandlerContext, packet: Packet): Data = packet match {
    case Packet(req, 1, _, Seq(Record(0, data))) if req > 0 =>
      val (handler, id) = data match {
        case Cluster(UInt(ver), Str(name)) =>
          val id = hub.allocateClientId(name)
          val handler = new ClientHandler(hub, tracker, messager, ctx.channel, id, name)
          hub.connectClient(id, name, handler)
          (handler, id)

        case Cluster(UInt(ver), Str(name), Str(doc)) =>
          val id = hub.allocateServerId(name)
          val handler = new ServerHandler(hub, tracker, messager, ctx.channel, id, name, doc)
          hub.connectServer(id, name, handler)
          (handler, id)

        // TODO: remove this case (collapse doc and notes)
        case Cluster(UInt(ver), Str(name), Str(docOrig), Str(notes)) =>
          val doc = if (notes.isEmpty) docOrig else (docOrig + "\n\n" + notes)
          val id = hub.allocateServerId(name)
          val handler = new ServerHandler(hub, tracker, messager, ctx.channel, id, name, doc)
          hub.connectServer(id, name, handler)
          (handler, id)

        case _ =>
          throw LabradException(1, "Invalid identification packet")
      }

      // logged in successfully; add new handler to channel pipeline
      val pipeline = ctx.pipeline
      pipeline.addLast("contextCodec", new ContextCodec(id))
      pipeline.addLast(handler.getClass.toString, handler)
      pipeline.remove(this)

      UInt(id)

    case _ =>
      throw LabradException(1, "Invalid identification packet")
  }
}
