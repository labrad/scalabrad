package org.labrad

import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import org.labrad.annotations.IsServer
import org.labrad.data._
import org.labrad.errors._
import org.labrad.events._
import org.labrad.util._
import scala.collection._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success}


class ServerConnection[T <: ServerContext : ClassTag : TypeTag](
  val name: String,
  val doc: String,
  val server: Server[T],
  val host: String,
  val port: Int,
  val password: Array[Char]
) extends Connection with Logging {

  private val nextMessageId = new AtomicInteger(1)
  def getMessageId = nextMessageId.getAndIncrement()

  def serve()(implicit timeout: Duration = 30.seconds): Unit = {
    server.init(this)

    val msgId = getMessageId
    addMessageListener {
      case Message(`msgId`, context, _, _) =>
        contexts.remove(context).map { case (instance, ctxMgr) => ctxMgr.expire() }
    }

    Await.result(
      for {
        _ <- registerSettings()
        _ <- send("Manager", "S: Notify on Context Expiration" -> Cluster(UInt(msgId), Bool(false)))
        _ <- send("Manager", "S: Start Serving" -> Data.NONE)
      } yield (),
      timeout
    )
    log.info("Now serving...")
  }

  override protected def handleRequest(packet: Packet): Unit = {
    val cls = implicitly[ClassTag[T]].runtimeClass
    val ctor = cls.getConstructors()(0)
    val (_, ctxMgr) = contexts.getOrElseUpdate(packet.context, {
      val ctxObj = ctor.newInstance(Array[java.lang.Object](this, server, packet.context): _*).asInstanceOf[T]
      val ctxMgr = new ContextMgr[T](ctxObj, serverCtx => handler(serverCtx))
      (ctxObj, ctxMgr)
    })
    ctxMgr.serve(packet).onComplete {
      case Success(response) => sendPacket(response)
      case Failure(e) => // should not happen
    }
  }

  def triggerShutdown(): Unit = {
    val expirations = contexts.values.map { case (instance, ctxMgr) => ctxMgr.expire() }
    contexts.clear()
    Await.result(Future.sequence(expirations), 60.seconds)
    server.shutdown()
    close()
  }


  // request serving

  protected def loginData =
    Cluster(UInt(Client.PROTOCOL_VERSION), Str(name), Str(doc.stripMargin), Str(""))

  private val contexts = mutable.Map.empty[Context, (T, ContextMgr[T])]
  private val (settings, handler): (Seq[SettingInfo], T => RequestContext => Data) = Reflect.makeHandler[T]

  def get(context: Context): Option[T] = contexts.get(context).map(_._1)

  private def registerSettings(): Future[Unit] = {
    val registrations = settings.sortBy(_.id).map(s =>
      Cluster(
        UInt(s.id),
        Str(s.name),
        Str(s.doc),
        Arr(s.accepts.expand.map(_.toString).map(Str(_))), // TODO: when manager supports patterns, no need for expand
        Arr(s.returns.expand.map(_.toString).map(Str(_))),
        Str(""))
    )
    send("Manager", registrations.map("S: Register Setting" -> _): _*).map(_ => ())
  }
}

object ServerConnection extends Logging {
  /** Create a new server connection that will use a particular context server object. */
  def apply[T <: ServerContext : ClassTag : TypeTag](server: Server[T], host: String, port: Int, password: Array[Char]) = {
    val (name, doc) = checkAnnotation(server)
    new ServerConnection[T](name, doc, server, host, port, password)
  }

  private def checkAnnotation[T <: ServerContext](server: Server[T]): (String, String) = {
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
