package org.labrad

import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import org.labrad.annotations.IsServer
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
  val server: Server[_, _],
  val host: String,
  val port: Int,
  val password: Array[Char]
) extends Connection with Logging {

  private val nextMessageId = new AtomicInteger(1)
  def getMessageId = nextMessageId.getAndIncrement()

  def serve()(implicit timeout: Duration = 30.seconds): Unit = {
    server.doInit(this)
  }

  override protected def handleRequest(packet: Packet): Unit = {
    server.handleRequest(packet).onComplete {
      case Success(response) => sendPacket(response)
      case Failure(e) => // should not happen
    }
  }

  private val shutdownPromise = Promise[Unit]

  def triggerShutdown(): Unit = {
    try {
      server.doShutdown()
      close()
      shutdownPromise.success(())
    } catch {
      case e: Exception =>
        shutdownPromise.failure(e)
    }
  }

  def awaitShutdown(timeout: Duration = Duration.Inf): Unit = {
    Await.result(shutdownPromise.future, timeout)
  }


  // request serving

  protected def loginData =
    Cluster(UInt(Client.PROTOCOL_VERSION), Str(name), Str(doc.stripMargin), Str(""))
}

object ServerConnection extends Logging {
  /** Create a new server connection that will use a particular context server object. */
  def apply[S <: Server[S, _], T <: ServerContext](server: Server[S, T], host: String, port: Int, password: Array[Char]) = {
    val (name, doc) = checkAnnotation(server)
    new ServerConnection(name, doc, server, host, port, password)
  }

  private def checkAnnotation(server: Server[_, _]): (String, String) = {
    val cls = server.getClass
    if (!cls.isAnnotationPresent(classOf[IsServer]))
      sys.error(s"Server class '${cls.getName}' lacks @ServerInfo annotation.")
    val info = cls.getAnnotation(classOf[IsServer])
    var name = info.name
    val doc = info.doc

    // interpolate environment vars

    // find all environment vars in the string
    val p = Pattern.compile("%([^%]*)%")
    val m = p.matcher(name)
    val keys = {
      val keys = Seq.newBuilder[String]
      while (m.find) keys += m.group(1)
      keys.result
    }

    // substitute environment variable into string
    for (key <- keys; value <- sys.env.get(key)) {
      log.info(key + " -> " + value)
      name = name.replaceAll("%" + key + "%", value)
    }

    (name, doc)
  }
}
