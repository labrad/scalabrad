package org.labrad

import org.labrad.data._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

trait Requester {
  def call(setting: String, data: Data*)(implicit ec: ExecutionContext): Future[Data]
  def callUnit(setting: String, data: Data*)(implicit ec: ExecutionContext): Future[Unit] = call(setting, data: _*).map { _ => () }
}


abstract class ServerProxy(val cxn: Connection, val name: String) extends Requester {
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


class GenericProxy(cxn: Connection, name: String) extends ServerProxy(cxn, name) {
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

class ManagerServerProxy(cxn: Connection, name: String = "Manager")
    extends ServerProxy(cxn, name) with ManagerServer {
  def packet(ctx: Context) = new ManagerServerPacket(this, ctx)
}

class ManagerServerPacket(server: ServerProxy, ctx: Context)
  extends PacketProxy(server, ctx) with ManagerServer


trait RegistryServer extends Requester {
  def dir()(implicit ec: ExecutionContext): Future[(Seq[String], Seq[String])] =
    call("dir").map { case Cluster(dirs, keys) => (dirs.getStringSeq, keys.getStringSeq) }

  def cd(dir: String)(implicit ec: ExecutionContext): Future[Seq[String]] = call("cd", Str(dir)).map { _.getStringSeq }
  def cd(dir: Seq[String])(implicit ec: ExecutionContext): Future[Seq[String]] = call("cd", Arr(dir.map(Str(_)))).map { _.getStringSeq }

  def mkDir(dir: String)(implicit ec: ExecutionContext): Future[Seq[String]] = call("mkdir", Str(dir)).map { _.getStringSeq }
  def rmDir(dir: String)(implicit ec: ExecutionContext): Future[Unit] = callUnit("rmdir", Str(dir))

  def get(key: String)(implicit ec: ExecutionContext): Future[Data] = call("get", Str(key))
  def set(key: String, value: Data)(implicit ec: ExecutionContext): Future[Unit] = callUnit("set", Str(key), value)
  def del(key: String)(implicit ec: ExecutionContext): Future[Unit] = callUnit("del", Str(key))
}

class RegistryServerProxy(cxn: Connection, name: String = "Registry")
    extends ServerProxy(cxn, name) with RegistryServer {
  def packet(ctx: Context) = new RegistryServerPacket(this, ctx)
}

class RegistryServerPacket(server: ServerProxy, ctx: Context)
  extends PacketProxy(server, ctx) with RegistryServer
