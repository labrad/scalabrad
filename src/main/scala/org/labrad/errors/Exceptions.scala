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
package errors

import data._
import types.Type


class IncorrectPasswordException extends Exception
class LoginFailedException(cause: Throwable) extends Exception(cause)
class LookupFailedException extends Exception
class NonIndexableTypeException(typ: Type) extends Exception("'" + typ + "' cannot be indexed.")

class LabradException(val code: Int, val message: String, val payload: Data, cause: Throwable) extends Exception {
  def this(code: Int, message: String, cause: Throwable) = this(code, message, Data.EMPTY, cause)
  def this(code: Int, message: String, payload: Data) = this(code, message, payload, null)
  def this(code: Int, message: String) = this(code, message, Data.EMPTY)
  def this(error: Data) = this(error.getErrorCode, error.getErrorMessage, error.getErrorPayload)
  
  def toData: Data = Error(code, message, payload)
}
