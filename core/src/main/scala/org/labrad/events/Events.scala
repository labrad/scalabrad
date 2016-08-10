package org.labrad.events

import java.util.{EventObject, EventListener}
import org.labrad.data._
import scala.collection._

class Listeners[T <: EventListener](source: AnyRef) {
  protected val listeners = mutable.Buffer.empty[T]

  def +=(listener: T) { if (!listeners.contains(listener)) listeners += listener }
  def -=(listener: T) { if (listeners.contains(listener)) listeners -= listener }

  protected def dispatch(f: T => Unit) { listeners foreach f }
}


@SerialVersionUID(1L)
class ConnectionEvent(source: AnyRef) extends EventObject(source)

trait ConnectionListener extends EventListener {
  def connected(evt: ConnectionEvent)
  def disconnected(evt: ConnectionEvent)
}

class ConnectionListeners(source: AnyRef) extends Listeners[ConnectionListener](source) {
  def fireConnected = dispatch(_.connected(new ConnectionEvent(source)))
  def fireDisconnected = dispatch(_.disconnected(new ConnectionEvent(source)))
}


@SerialVersionUID(1L)
case class ContextEvent(src: AnyRef) extends EventObject(src)

trait ContextListener extends EventListener {
  def newContext(evt: ContextEvent)
  def expireContext(evt: ContextEvent)
}

class ContextListeners(val source: AnyRef) extends Listeners[ContextListener](source) {
  def fireNewContext = dispatch(_.newContext(new ContextEvent(source)))
  def fireDisconnected = dispatch(_.expireContext(new ContextEvent(source)))
}


@SerialVersionUID(1L)
case class MessageEvent(src: AnyRef, context: Context, srcId: Long, msgId: Long, data: Data) extends EventObject(src)

trait MessageListener extends EventListener {
  def onMessage(e: MessageEvent)
}

class MessageListeners(source: AnyRef) extends Listeners[MessageListener](source) {
  def fireMessage(packet: Packet) {
    val ctx = packet.context
    val src = packet.target
    for (Record(id, data) <- packet.records)
      dispatch { _.onMessage(new MessageEvent(source, ctx, src, id, data)) }
  }
}


