package org.labrad

import org.labrad.data._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

trait Requester {
  implicit def executionContext: ExecutionContext

  def cxn: Connection
  def context: Context

  def call(setting: String, data: Data*): Future[Data]
  def callUnit(setting: String, data: Data*): Future[Unit] = call(setting, data: _*).map { _ => () }
  def call[T](setting: String, data: Data*)(implicit getter: Getter[T]): Future[T] = call(setting, data: _*).map { result => getter.get(result) }
}


abstract class ServerProxy(val cxn: Connection, val name: String, val context: Context) extends Requester {
  implicit def executionContext = cxn.executionContext

  override def call(setting: String, args: Data*): Future[Data] = {
    val data = args match {
      case Seq() => Data.NONE
      case Seq(data) => data
      case args => Cluster(args: _*)
    }
    cxn.send(name, context, setting -> data).map(_(0))
  }
}

class PacketProxy(val server: ServerProxy, val context: Context) extends Requester {
  implicit def executionContext = server.executionContext

  def cxn = server.cxn

  private val records = mutable.Buffer.empty[(String, Data)]
  private val promise = Promise[Seq[Data]]

  override def call(setting: String, args: Data*): Future[Data] = {
    val idx = records.size
    val data = args match {
      case Seq() => Data.NONE
      case Seq(data) => data
      case args => Cluster(args: _*)
    }
    records += setting -> data
    promise.future.map(_(idx))
  }

  def send(): Future[Unit] = {
    promise.completeWith(server.cxn.send(server.name, context, records: _*))
    promise.future.map(_ => ())
  }
}


class GenericProxy(cxn: Connection, name: String, context: Context = Context(0, 0)) extends ServerProxy(cxn, name, context) {
  def packet(ctx: Context = context) = new PacketProxy(this, ctx)
}


trait ManagerServer extends Requester {
  def servers(): Future[Seq[(Long, String)]] =
    call[Seq[(Long, String)]]("Servers")

  def settings(server: String): Future[Seq[(Long, String)]] =
    call[Seq[(Long, String)]]("Settings", Str(server))

  def lookupServer(name: String): Future[Long] =
    call[Long]("Lookup", Str(name))

  def serverHelp(server: String): Future[(String, String)] =
    call[(String, String)]("Help", Str(server))

  def settingHelp(server: String, setting: String): Future[(String, Seq[String], Seq[String], String)] =
    call[(String, Seq[String], Seq[String], String)]("Help", Str(server), Str(setting))

  def dataToString(data: Data): Future[String] =
    call[String]("Data To String", data)

  def stringToData(s: String): Future[Data] =
    call("String To Data", Str(s))

  def subscribeToNamedMessage(name: String, msgId: Long, active: Boolean): Future[Unit] =
    callUnit("Subscribe to Named Message", Cluster(Str(name), UInt(msgId), Bool(active)))

  def connectionInfo(): Future[Seq[(Long, String, Boolean, Long, Long, Long, Long, Long, Long)]] =
    call[Seq[(Long, String, Boolean, Long, Long, Long, Long, Long, Long)]]("Connection Info")
}

class ManagerServerProxy(cxn: Connection, name: String = "Manager", context: Context = Context(0, 0))
extends ServerProxy(cxn, name, context) with ManagerServer {
  def packet(ctx: Context = context) = new ManagerServerPacket(this, ctx)
}

class ManagerServerPacket(server: ServerProxy, ctx: Context)
extends PacketProxy(server, ctx) with ManagerServer


trait RegistryServer extends Requester {
  def dir(): Future[(Seq[String], Seq[String])] =
    call[(Seq[String], Seq[String])]("dir")

  def cd(dir: String): Future[Seq[String]] = call[Seq[String]]("cd", Str(dir))
  def cd(dir: Seq[String], create: Boolean = false): Future[Seq[String]] = call[Seq[String]]("cd", Arr(dir.map(Str(_))), Bool(create))

  def mkDir(dir: String): Future[Seq[String]] = call[Seq[String]]("mkdir", Str(dir))
  def rmDir(dir: String): Future[Unit] = callUnit("rmdir", Str(dir))

  def get(key: String, pat: String = "?", default: Option[(Boolean, Data)] = None): Future[Data] = {
    default match {
      case Some((set, default)) => call("get", Str(key), Str(pat), Bool(set), default)
      case None => call("get", Str(key), Str(pat))
    }
  }
  def set(key: String, value: Data): Future[Unit] = callUnit("set", Str(key), value)
  def del(key: String): Future[Unit] = callUnit("del", Str(key))

  def notifyOnChange(id: Long, enable: Boolean): Future[Unit] =
    callUnit("Notify on Change", UInt(id), Bool(enable))
}

class RegistryServerProxy(cxn: Connection, name: String = "Registry", context: Context = Context(0, 0))
extends ServerProxy(cxn, name, context) with RegistryServer {
  def packet(ctx: Context = context) = new RegistryServerPacket(this, ctx)
}

class RegistryServerPacket(server: ServerProxy, ctx: Context)
extends PacketProxy(server, ctx) with RegistryServer
