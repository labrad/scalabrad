package org.labrad.errors

import org.labrad.data._
import org.labrad.types.Type

class IncorrectPasswordException extends Exception
class LoginFailedException(cause: Throwable) extends Exception(cause)
class LookupFailedException extends Exception
class NonIndexableTypeException(typ: Type) extends Exception(s"'$typ' cannot be indexed.")

case class LabradException(code: Int, message: String, payload: Data, cause: Throwable)
extends Exception(s"$code: $message", cause) {
  def this(code: Int, message: String, cause: Throwable) = this(code, message, Data.NONE, cause)
  def this(code: Int, message: String, payload: Data) = this(code, message, payload, null)
  def this(code: Int, message: String) = this(code, message, Data.NONE)
  def this(error: Data) = this(error.getErrorCode, error.getErrorMessage, error.getErrorPayload)

  def toData: Data = Error(code, message, payload)
}

object LabradException {
  def apply(code: Int, message: String) = new LabradException(code, message)
  def apply(error: Data) = new LabradException(error.getErrorCode, error.getErrorMessage, error.getErrorPayload)
}
