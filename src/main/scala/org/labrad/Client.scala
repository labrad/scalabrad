package org.labrad

import org.labrad.data._

class Client(
  val name: String = "Scala Client",
  val host: String = "localhost" /*Util.getEnv("LABRADHOST", "localhost")*/,
  val port: Int = 7682 /*Util.getEnvInt("LABRADPORT", 7682)*/,
  val password: String = "" /*Util.getEnv("LABRADPASSWORD", "")*/
) extends Connection {
  protected def loginData = Cluster(UInt(Client.PROTOCOL_VERSION), Str(name))
}

object Client {
  val PROTOCOL_VERSION = 1L
}
