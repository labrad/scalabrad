package org.labrad

import java.io.{PrintWriter, StringWriter}
import org.labrad.data._
import org.labrad.errors.LabradException
import org.labrad.util.Util
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Success, Failure}

abstract class Server[T <: ServerContext : ClassTag : TypeTag] {

  def init(cxn: ServerConnection[T]): Unit
  def shutdown(): Unit

  def run(args: Array[String]): Unit = {
    val options = Util.parseArgs(args, Seq("host", "port", "password"))

    val host = options.get("host").orElse(sys.env.get("LABRADHOST")).getOrElse("localhost")
    val port = options.get("port").orElse(sys.env.get("LABRADPORT")).map(_.toInt).getOrElse(7682)
    val password = options.get("password").orElse(sys.env.get("LABRADPASSWORD")).getOrElse("").toCharArray

    val cxn = ServerConnection[T](this, host, port, password)
    cxn.connect()
    sys.ShutdownHookThread(cxn.triggerShutdown)
    cxn.serve()
  }
}

abstract class ServerContext(val cxn: Connection, val server: Server[_], val context: Context) {
  def init(): Unit
  def expire(): Unit
}

object Server {
  def handle(packet: Packet)(f: RequestContext => Data): Packet = {
    val Packet(request, source, context, records) = packet

    def process(records: Seq[Record]): Seq[Record] = records match {
      case Seq(Record(id, data), tail @ _*) =>
        val resp = try {
          f(RequestContext(source, context, id, data))
        } catch {
          case ex: Throwable =>
            val sw = new StringWriter
            ex.printStackTrace(new PrintWriter(sw))
            Error(0, sw.toString)
        }
        Record(id, resp) +: (if (resp.isError) Seq() else process(tail))

      case Seq() =>
        Seq()
    }
    Packet(-request, source, context, process(records))
  }

  def handleAsync(packet: Packet)(f: RequestContext => Future[Data])(implicit ec: ExecutionContext): Future[Packet] = {
    val Packet(request, source, context, records) = packet

    def process(records: Seq[Record]): Future[Seq[Record]] = records match {
      case Seq(Record(id, data), tail @ _*) =>
        f(RequestContext(source, context, id, data)) recover {
          case ex =>
            val sw = new StringWriter
            ex.printStackTrace(new PrintWriter(sw))
            Error(0, sw.toString)
        } flatMap {
          case resp if resp.isError =>
            Future.successful { Record(id, resp) +: Seq() }
          case resp =>
            process(tail) map { Record(id, resp) +: _ }
        }

      case Seq() =>
        Future.successful(Seq())
    }
    process(records) map {
      Packet(-request, source, context, _)
    }
  }
}
