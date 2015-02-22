package org.labrad

import org.labrad.data._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

trait Requester {
  def call(setting: String, data: Data*)(implicit ec: ExecutionContext): Future[Data]
  def callUnit(setting: String, data: Data*)(implicit ec: ExecutionContext): Future[Unit] = call(setting, data: _*).map { _ => () }
}


abstract class ServerProxy(val cxn: Connection, val name: String, val context: Context) extends Requester { self =>
  override def call(setting: String, args: Data*)(implicit ec: ExecutionContext): Future[Data] = {
    val data = args match {
      case Seq() => Data.NONE
      case Seq(data) => data
      case args => Cluster(args: _*)
    }
    cxn.send(name, context, setting -> data).map(_(0))
  }
}


class GenericProxy(cxn: Connection, name: String, context: Context) extends ServerProxy(cxn, name, context) {
  def packet(ctx: Context = context) = new PacketProxy(this, ctx)
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
    records += setting -> data
    promise.future.map(_(idx))
  }

  def send(implicit ec: ExecutionContext): Future[Unit] = {
    promise.completeWith(server.cxn.send(server.name, ctx, records: _*))
    promise.future.map(_ => ())
  }
}


trait ManagerServer extends Requester {
  def servers(implicit ec: ExecutionContext): Future[Seq[(Long, String)]] =
    call("Servers").map { _.get[Seq[(Long, String)]] }

  def settings(server: String)(implicit ec: ExecutionContext): Future[Seq[(Long, String)]] =
    call("Settings", Str(server)).map { _.get[Seq[(Long, String)]] }

  def lookupServer(name: String)(implicit ec: ExecutionContext): Future[Long] =
    call("Lookup", Str(name)).map { _.get[Long] }

  def dataToString(data: Data)(implicit ec: ExecutionContext): Future[String] =
    call("Data To String", data).map { _.get[String] }

  def stringToData(s: String)(implicit ec: ExecutionContext): Future[Data] =
    call("String To Data", Str(s))

  def subscribeToNamedMessage(name: String, msgId: Long, active: Boolean)(implicit ec: ExecutionContext): Future[Unit] =
    callUnit("Subscribe to Named Message", Cluster(Str(name), UInt(msgId), Bool(active)))

  def connectionInfo()(implicit ec: ExecutionContext): Future[Seq[(Long, String, Boolean, Long, Long, Long, Long, Long, Long)]] =
    call("Connection Info").map { _.get[Seq[(Long, String, Boolean, Long, Long, Long, Long, Long, Long)]] }
}

class ManagerServerProxy(cxn: Connection, name: String = "Manager", context: Context = Context(0, 0))
    extends ServerProxy(cxn, name, context) with ManagerServer {
  def packet(ctx: Context = context) = new ManagerServerPacket(this, ctx)
}

class ManagerServerPacket(server: ServerProxy, ctx: Context)
  extends PacketProxy(server, ctx) with ManagerServer


trait RegistryServer extends Requester {
  def dir()(implicit ec: ExecutionContext): Future[(Seq[String], Seq[String])] =
    call("dir").map { _.get[(Seq[String], Seq[String])] }

  def cd(dir: String)(implicit ec: ExecutionContext): Future[Seq[String]] = call("cd", Str(dir)).map { _.get[Seq[String]] }
  def cd(dir: Seq[String], create: Boolean = false)(implicit ec: ExecutionContext): Future[Seq[String]] = call("cd", Arr(dir.map(Str(_))), Bool(create)).map { _.get[Seq[String]] }

  def mkDir(dir: String)(implicit ec: ExecutionContext): Future[Seq[String]] = call("mkdir", Str(dir)).map { _.get[Seq[String]] }
  def rmDir(dir: String)(implicit ec: ExecutionContext): Future[Unit] = callUnit("rmdir", Str(dir))

  def get(key: String)(implicit ec: ExecutionContext): Future[Data] = call("get", Str(key))
  def set(key: String, value: Data)(implicit ec: ExecutionContext): Future[Unit] = callUnit("set", Str(key), value)
  def del(key: String)(implicit ec: ExecutionContext): Future[Unit] = callUnit("del", Str(key))

  def notifyOnChange(id: Long, enable: Boolean)(implicit ec: ExecutionContext): Future[Unit] =
    callUnit("Notify On Change", UInt(id), Bool(enable))
}

class RegistryServerProxy(cxn: Connection, name: String = "Registry", context: Context = Context(0, 0))
    extends ServerProxy(cxn, name, context) with RegistryServer {
  def packet(ctx: Context = context) = new RegistryServerPacket(this, ctx)
}

class RegistryServerPacket(server: ServerProxy, ctx: Context)
  extends PacketProxy(server, ctx) with RegistryServer
