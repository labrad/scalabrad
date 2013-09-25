package org.labrad

import org.labrad.data._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

trait Requester {
  def call(setting: String, data: Data*)(implicit ec: ExecutionContext): Future[Data]
}


abstract class ServerProxy(val name: String, val cxn: Connection) extends Requester {
  override def call(setting: String, args: Data*)(implicit ec: ExecutionContext): Future[Data] = {
    val data = args match {
      case Seq() => Data.NONE
      case Seq(data) => data
      case args => Cluster(args: _*)
    }
    cxn.send(name, setting -> data).map(_(0))
  }

  def newContext = cxn.newContext
}


class GenericProxy(name: String, cxn: Connection) extends ServerProxy(name, cxn) {
  def packet = new PacketProxy(this, Context(0, 0))
  def packet(ctx: Context) = new PacketProxy(this, ctx)
}


class PacketProxy(server: ServerProxy, ctx: Context) extends Requester {
  private val records = mutable.Buffer.empty[(String, Data)]
  private val promise = Promise[Seq[Data]]

  override def call(setting: String, args: Data*)(implicit ec: ExecutionContext): Future[Data] = {
    val idx = records.size
    val data = args match {
      case Seq() => Data.NONE
      case Seq(data) => data
      case args => Cluster(args: _*)
    }
    records += ((setting, data))
    promise.future.map(_(idx))
  }

  def send(implicit ec: ExecutionContext): Future[Unit] = {
    promise.completeWith(server.cxn.send(server.name, ctx, records: _*))
    promise.future.map(_ => ())
  }
}


trait ManagerServer extends Requester {
  def servers(implicit ec: ExecutionContext): Future[Seq[(Long, String)]] =
    call("Servers").map {
      _.getDataSeq.map { case Cluster(UInt(id), Str(name)) => (id, name) }
    }

  def settings(server: String)(implicit ec: ExecutionContext): Future[Seq[(Long, String)]] =
    call("Settings", Str(server)).map {
      _.getDataSeq.map { case Cluster(UInt(id), Str(name)) => (id, name) }
    }

  def lookupServer(name: String)(implicit ec: ExecutionContext): Future[Long] =
    call("Lookup", Str(name)).map { case UInt(id) => id }

  def dataToString(data: Data)(implicit ec: ExecutionContext): Future[String] =
    call("Data To String", data).map { case Str(s) => s }

  def stringToData(s: String)(implicit ec: ExecutionContext): Future[Data] =
    call("String To Data", Str(s))

  def subscribeToNamedMessage(name: String, msgId: Long, active: Boolean)(implicit ec: ExecutionContext): Future[Unit] =
    call("Subscribe to Named Message", Cluster(Str(name), UInt(msgId), Bool(active))).map(_ => ())
}

class ManagerServerProxy(name: String, cxn: Connection)
    extends ServerProxy(name, cxn) with ManagerServer {
  def packet(ctx: Context) = new ManagerServerPacket(this, ctx)
}

class ManagerServerPacket(server: ServerProxy, ctx: Context)
  extends PacketProxy(server, ctx) with ManagerServer
