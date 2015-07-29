package org.labrad

import java.io.File
import org.labrad.data._
import org.labrad.util.Logging
import scala.concurrent.Future
import scala.util.{Failure, Success}


class ServerConnection(
  val name: String,
  val doc: String,
  val host: String,
  val port: Int,
  val password: Array[Char],
  val tls: TlsMode = TlsMode.STARTTLS,
  val tlsCerts: Map[String, File] = Map(),
  handler: Packet => Future[Packet]
) extends Connection with Logging {

  protected def loginData = Cluster(
    UInt(Client.PROTOCOL_VERSION),
    Str(name),
    Str(doc.stripMargin),
    Str("")
  )

  override protected def handleRequest(packet: Packet): Unit = {
    handler(packet).onComplete {
      case Success(response) => sendPacket(response)
      case Failure(e) => // should not happen
    }
  }
}
