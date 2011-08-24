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

import scala.collection._

import grizzled.slf4j.Logging

import data._


class RequestDispatcher(send: Packet => Unit) extends Logging {
  private var _nextId = 1
  private def nextId = { _nextId += 1; _nextId }
  private val idPool = mutable.Buffer.empty[Int]
  private val receivers = mutable.Map.empty[Int, RequestReceiver]


  /**
   * Start a request with an optional callback.
   * @param request the request to be sent
   * @param callback a callback to be called when the request finishes
   * @return a receiver for getting the result
   */
  def startRequest(request: Request, callback: Option[RequestCallback] = None): () => Seq[Data] =
    synchronized {
      val id = if (idPool.isEmpty) nextId else idPool.remove(0)
      val receiver = new RequestReceiver(request, callback)
      receivers(id) = receiver
      send(Packet.forRequest(request, id))
      () => receiver.get
    }

  /**
   * Finish a request from an incoming response packet.
   * @param packet the received response packet
   */
  def finishRequest(packet: Packet) {
    synchronized {
      val id = -packet.id
      receivers.get(id) match {
        case Some(receiver) =>
          // response to a request we made
          receivers -= id
          idPool += id
          receiver.set(packet)
        case None =>
          // response to a request we didn't make
          warn("Received a response to an unknown request: %s.".format(id))
      }
    }
  }

  /**
   * Cause all pending requests to fail.  This is called when the
   * connection using this dispatcher is closed.
   * @param cause of the failure
   */
  def failAll(cause: Throwable) {
    synchronized {
      for ((id, receiver) <- receivers) {
        idPool += id
        receiver.fail(cause)
      }
      receivers.clear
    }
  }
}
