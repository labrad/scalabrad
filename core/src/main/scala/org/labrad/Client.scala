package org.labrad

import io.netty.channel.EventLoopGroup
import java.io.File
import org.labrad.data._
import scala.concurrent.ExecutionContext

class Client(
  val name: String = Client.defaults.name,
  val host: String = Client.defaults.host,
  val port: Int = Client.defaults.port,
  val credential: Credential = Client.defaults.credential,
  val tls: TlsMode = Client.defaults.tls,
  val tlsCerts: Map[String, File] = Map(),
  val workerGroup: EventLoopGroup = Connection.defaultWorkerGroup,
  val requireSrp: Boolean = false
)(
  implicit val executionContext: ExecutionContext = ExecutionContext.global
) extends Connection {
  protected def loginData = Cluster(UInt(Client.PROTOCOL_VERSION), Str(name))
}

object Client {
  val PROTOCOL_VERSION = 1L

  object defaults {
    def name: String = "Scala Client"
    def host: String = sys.env.getOrElse("LABRADHOST", "localhost")
    def username: String = sys.env.getOrElse("LABRADUSER", "")
    def password: Array[Char] = sys.env.getOrElse("LABRADPASSWORD", "").toCharArray
    def credential: Credential = Password(username, password)
    def port: Int = tls match {
      case TlsMode.ON => sys.env.getOrElse("LABRAD_TLS_PORT", "7643").toInt
      case _          => sys.env.getOrElse("LABRADPORT", "7682").toInt
    }
    def tls: TlsMode = sys.env.get("LABRAD_TLS").map(TlsMode.fromString).getOrElse(TlsMode.STARTTLS)
  }
}
