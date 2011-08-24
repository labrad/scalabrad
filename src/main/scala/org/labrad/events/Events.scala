/*
 * Copyright 2008 Matthew Neeley
 * 
 * This file is part of JLabrad.
 *
 * JLabrad is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 * 
 * JLabrad is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with JLabrad.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.labrad
package events

import java.util.{EventObject, EventListener}

import scala.collection._
import scala.collection.JavaConversions._

import data._

class Listeners[T <: EventListener](source: AnyRef) {
  protected val listeners = mutable.Buffer.empty[T]

  def +=(listener: T) { if (!listeners.contains(listener)) listeners.add(listener) }
  def -=(listener: T) { if (listeners.contains(listener)) listeners.remove(listener) }
  
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


