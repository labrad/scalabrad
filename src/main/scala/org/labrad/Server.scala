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

import data.{Data, Context}


trait Server {  
  var connection: ServerConnection = _

  type ContextType <: ServerContext
  val contextClass: Class[ContextType]
  
  def getServerContext(context: Context): ContextType =
    contextClass.cast(connection.getServerContext(context))

  def init: Unit
  def shutdown: Unit
  
  def main(args: Array[String]) {
    val cxn = ServerConnection(this, contextClass)
    cxn.connect
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run { cxn.triggerShutdown }
    })
    cxn.serve
  }
}

abstract class ServerContext(val connection: Connection, val server: Server, val context: Context) {
  var source: Long = _

  def init: Unit
  def expire: Unit
}
