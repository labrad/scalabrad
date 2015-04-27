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
    call("Servers").map { _.get[Seq[(Long, String)]] }

  def settings(server: String): Future[Seq[(Long, String)]] =
    call("Settings", Str(server)).map { _.get[Seq[(Long, String)]] }

  def lookupServer(name: String): Future[Long] =
    call("Lookup", Str(name)).map { _.get[Long] }

  def serverHelp(server: String): Future[(String, String)] =
    call("Help", Str(server)).map { _.get[(String, String)] }

  def settingHelp(server: String, setting: String): Future[(String, Seq[String], Seq[String], String)] =
    call("Help", Str(server), Str(setting)).map { _.get[(String, Seq[String], Seq[String], String)] }

  def dataToString(data: Data): Future[String] =
    call("Data To String", data).map { _.get[String] }

  def stringToData(s: String): Future[Data] =
    call("String To Data", Str(s))

  def subscribeToNamedMessage(name: String, msgId: Long, active: Boolean): Future[Unit] =
    callUnit("Subscribe to Named Message", Cluster(Str(name), UInt(msgId), Bool(active)))

  def connectionInfo(): Future[Seq[(Long, String, Boolean, Long, Long, Long, Long, Long, Long)]] =
    call("Connection Info").map { _.get[Seq[(Long, String, Boolean, Long, Long, Long, Long, Long, Long)]] }
}

class ManagerServerProxy(cxn: Connection, name: String = "Manager", context: Context = Context(0, 0))
extends ServerProxy(cxn, name, context) with ManagerServer {
  def packet(ctx: Context = context) = new ManagerServerPacket(this, ctx)
}

class ManagerServerPacket(server: ServerProxy, ctx: Context)
extends PacketProxy(server, ctx) with ManagerServer


trait RegistryServer extends Requester {
  def dir(): Future[(Seq[String], Seq[String])] =
    call("dir").map { _.get[(Seq[String], Seq[String])] }

  def cd(dir: String): Future[Seq[String]] = call("cd", Str(dir)).map { _.get[Seq[String]] }
  def cd(dir: Seq[String], create: Boolean = false): Future[Seq[String]] = call("cd", Arr(dir.map(Str(_))), Bool(create)).map { _.get[Seq[String]] }

  def mkDir(dir: String): Future[Seq[String]] = call("mkdir", Str(dir)).map { _.get[Seq[String]] }
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
