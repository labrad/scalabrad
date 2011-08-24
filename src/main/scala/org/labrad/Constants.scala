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

import data.Context

object Constants {
  val DEFAULT_HOST = "localhost" /** Default hostname for the manager. */
  val DEFAULT_PORT = 7682 /** Default port to use when connecting to the manager. */
  val DEFAULT_PASSWORD = "" /** Default password to use when connecting to the manager. */
  val MANAGER = 1L /** ID of the LabRAD manager. */
  val SERVERS = 1L /** ID of the manager setting to retrieve a list of servers. */
  val SETTINGS = 2L /** ID of the manager setting to retrieve a settings list for a server. */
  val LOOKUP = 3L /** ID of the lookup setting on the manager. */
  val PROTOCOL = 1L /** Version number of the LabRAD protocol version implemented here. */
  val DEFAULT_CONTEXT = Context(0, 0) /** Default context in which requests will be sent. */
}
