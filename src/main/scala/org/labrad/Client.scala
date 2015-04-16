package org.labrad

import org.labrad.data._

class Client(
  val name: String = "Scala Client",
  val host: String = sys.env.getOrElse("LABRADHOST", ""),
  val port: Int = sys.env.getOrElse("LABRADPORT", "7682").toInt,
  val password: Array[Char] = sys.env.getOrElse("LABRADPASSWORD", "").toCharArray
) extends Connection {
  protected def loginData = Cluster(UInt(Client.PROTOCOL_VERSION), Str(name))
}

object Client {
  val PROTOCOL_VERSION = 1L
}
