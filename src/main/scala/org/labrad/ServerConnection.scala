package org.labrad

import org.labrad.data._
import org.labrad.errors._
import org.labrad.events._
import org.labrad.util._
import scala.collection._
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success}


class ServerConnection(
  val name: String,
  val doc: String,
  val host: String,
  val port: Int,
  val password: Array[Char],
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
