package org.labrad.manager

import org.labrad.data._
import org.labrad.util.Logging
import scala.collection.mutable

trait Message {
  def msgName: String
  def msgData: Data
}

trait Messager {
  def register(msg: String, target: Long, ctx: Context, id: Long): Unit
  def unregister(msg: String, target: Long, ctx: Context, id: Long): Unit
  def broadcast(msg: Message, sourceId: Long): Unit = broadcast(msg.msgName, msg.msgData, sourceId)
  def broadcast(msg: String, data: Data, sourceId: Long): Unit
  def disconnect(id: Long): Unit
}

class MessagerImpl(hub: Hub, tracker: StatsTracker) extends Messager with Logging {
  private var regs = mutable.Map.empty[String, mutable.Set[(Long, Context, Long)]]

  def register(msg: String, target: Long, ctx: Context, id: Long): Unit = {
    log.debug(s"subscribe to named message: msg=$msg, target=$target, ctx=$ctx, id=$id")
    synchronized {
      val listeners = regs.getOrElseUpdate(msg, mutable.Set.empty[(Long, Context, Long)])
      listeners += ((target, ctx, id))
    }
  }

  def unregister(msg: String, target: Long, ctx: Context, id: Long): Unit = {
    log.debug(s"unsubscribe from named message: msg=$msg, target=$target, ctx=$ctx, id=$id")
    synchronized {
      regs.get(msg) foreach { listeners =>
        listeners -= ((target, ctx, id))
        if (listeners.isEmpty)
          regs -= msg
      }
    }
  }

  def broadcast(msg: String, data: Data, src: Long): Unit = {
    log.debug(s"sending named message '$msg': src=$src, data=$data")
    val listenersOpt = synchronized {
      regs.get(msg).map(_.toSet)
    }
    for {
      listeners <- listenersOpt
      (target, ctx, id) <- listeners
    } {
      log.debug(s"named message recipient: target=${target}, ctx=${ctx}, id=${id}")
      tracker.msgSend(Manager.ID)
      hub.message(target, Packet(0, src, ctx, Seq(Record(id, data))))
    }
  }

  def disconnect(id: Long): Unit = {
    synchronized {
      val empties = Seq.newBuilder[String]
      for ((msg, listeners) <- regs) {
        listeners --= listeners filter { case (target, _, _) => target == id }
        if (listeners.isEmpty) {
          empties += msg
        }
      }
      for (msg <- empties.result) {
        regs -= msg
      }
    }
  }
}
