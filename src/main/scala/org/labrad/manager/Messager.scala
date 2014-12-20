package org.labrad.manager

import org.labrad.data._
import org.labrad.util.Logging
import scala.collection.mutable

trait Messager {
  def register(msg: String, target: Long, ctx: Context, id: Long)
  def unregister(msg: String, target: Long, ctx: Context, id: Long)
  def broadcast(msg: String, data: Data, sourceId: Long)
  def disconnect(id: Long)
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

  def broadcast(msg: String, data: Data, src: Long): Unit = synchronized {
    log.debug(s"sending named message '$msg': $data")
    regs.get(msg) foreach { listeners =>
      for ((target, ctx, id) <- listeners) {
        log.debug(s"named message recipient: target=${target}, ctx=${ctx}, id=${id}")
        hub.message(target, Packet(0, src, ctx, Seq(Record(id, data))))
      }
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
