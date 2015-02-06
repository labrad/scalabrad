package org.labrad.manager

import org.labrad.data._
import org.labrad.util.Logging
import scala.collection.mutable

trait Message {
  def name: String
  def data: Data
}

trait Messager {
  def register(msg: String, target: Long, ctx: Context, id: Long): Unit
  def unregister(msg: String, target: Long, ctx: Context, id: Long): Unit
  def broadcast(msg: Message, sourceId: Long): Unit = broadcast(msg.name, msg.data, sourceId)
  def broadcast(msg: String, data: Data, sourceId: Long): Unit
  def disconnect(id: Long): Unit
}

class MessagerImpl(hub: Hub) extends Messager with Logging {
  private var regs = mutable.Map.empty[String, mutable.Set[(Long, Context, Long)]]

  def register(msg: String, target: Long, ctx: Context, id: Long): Unit = synchronized {
    regs.get(msg) match {
      case Some(listeners) => listeners += ((target, ctx, id))
      case None => regs(msg) = mutable.Set((target, ctx, id))
    }
  }

  def unregister(msg: String, target: Long, ctx: Context, id: Long): Unit = synchronized {
    regs.get(msg) foreach { listeners =>
      listeners -= ((target, ctx, id))
      if (listeners.isEmpty)
        regs -= msg
    }
  }

  def broadcast(msg: String, data: Data, src: Long): Unit = {
    log.debug(s"sending named message '$msg': $data")
    val listenersOpt = synchronized {
      regs.get(msg).map(_.toSet)
    }
    for {
      listeners <- listenersOpt
      (target, ctx, id) <- listeners
    } {
      log.debug(s"named message recipient: target=${target}, ctx=${ctx}, id=${id}")
      hub.message(target, Packet(0, src, ctx, Seq(Record(id, data))))
    }
  }

  def disconnect(id: Long): Unit = synchronized {
    for ((msg, listeners) <- regs) {
      listeners --= listeners filter { case (target, _, _) => target == id }
      if (listeners.isEmpty)
        regs -= msg
    }
  }
}
