package org.labrad.util

import scala.reflect.ClassTag

class Logger(val logger: org.slf4j.Logger) {
  @inline final def name = logger.getName

  @inline final def isTraceEnabled = logger.isTraceEnabled
  @inline final def isDebugEnabled = logger.isDebugEnabled
  @inline final def isErrorEnabled = logger.isErrorEnabled
  @inline final def isInfoEnabled = logger.isInfoEnabled
  @inline final def isWarnEnabled = logger.isWarnEnabled

  @inline final def trace(msg: => Any): Unit = if (isTraceEnabled) logger.trace(toStr(msg))
  @inline final def debug(msg: => Any): Unit = if (isDebugEnabled) logger.debug(toStr(msg))
  @inline final def error(msg: => Any): Unit = if (isErrorEnabled) logger.error(toStr(msg))
  @inline final def info(msg: => Any): Unit = if (isInfoEnabled) logger.info(toStr(msg))
  @inline final def warn(msg: => Any): Unit = if (isWarnEnabled) logger.warn(toStr(msg))

  @inline final def trace(msg: => Any, t: => Throwable): Unit = if (isTraceEnabled) logger.trace(toStr(msg), t)
  @inline final def debug(msg: => Any, t: => Throwable): Unit = if (isDebugEnabled) logger.debug(toStr(msg), t)
  @inline final def error(msg: => Any, t: => Throwable): Unit = if (isErrorEnabled) logger.error(toStr(msg), t)
  @inline final def info(msg: => Any, t: => Throwable): Unit = if (isInfoEnabled) logger.info(toStr(msg), t)
  @inline final def warn(msg: => Any, t: => Throwable): Unit = if (isWarnEnabled) logger.warn(toStr(msg), t)

  private def toStr(msg: Any): String = msg match {
    case null => "<null>"
    case _    => msg.toString
  }
}

object Logger {
  val RootLoggerName = org.slf4j.Logger.ROOT_LOGGER_NAME

  def apply(name: String): Logger = new Logger(org.slf4j.LoggerFactory.getLogger(name))
  def apply(cls: Class[_]): Logger = apply(cls.getName)
  def apply[C](implicit m: ClassTag[C]): Logger = apply(m.runtimeClass.getName)

  def rootLogger = apply(RootLoggerName)
}

trait Logging {
  protected lazy val log = Logger(getClass)
}
