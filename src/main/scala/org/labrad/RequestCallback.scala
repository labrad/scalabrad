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

import data.{Data, Request}

trait RequestCallback {
  /**
   * Called when the request completes successfully.
   * @param request
   * @param response
   */
  def onSuccess(request: Request, response: Seq[Data]): Unit

  /**
   * Called when the request fails.
   * @param request
   * @param cause
   */
  def onFailure(request: Request, cause: Throwable): Unit
}
