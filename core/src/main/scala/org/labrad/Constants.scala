package org.labrad

object Labrad {
  val VERSION = {
    val url = getClass.getResource("/org/labrad/version.txt")
    scala.io.Source.fromURL(url).mkString
  }

  object Manager {
    val ID = 1L
    val NAME = "Manager"

    // setting ids
    val SERVERS = 1L
    val SETTINGS = 2L
    val LOOKUP = 3L
  }

  object Authenticator {
    val NAME = "Auth"
    val METHODS_SETTING_ID = 101
    val INFO_SETTING_ID = 102
    val AUTH_SETTING_ID = 103
  }
}
