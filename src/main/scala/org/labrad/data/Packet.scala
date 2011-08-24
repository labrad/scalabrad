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
package data

import Constants.DEFAULT_CONTEXT


case class Packet(id: Int, target: Long, context: Context, records: Seq[Record])

object Packet {
  def forRequest(request: Request, requestNum: Int) = request match {
    case Request(id, context, records) => Packet(requestNum, id, context, records)
  }

  def forMessage(request: Request) = forRequest(request, 0)
}

case class Record(id: Long, data: Data = Data.EMPTY)
case class Request(server: Long, context: Context = DEFAULT_CONTEXT, records: Seq[Record] = Seq.empty)

case class NameRecord(name: String, data: Data = Data.EMPTY)
case class NameRequest(server: String, context: Context = DEFAULT_CONTEXT, records: Seq[NameRecord] = Seq.empty)

case class Message(source: Long, context: Context, msg: Long, data: Data)

