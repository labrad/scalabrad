package org.labrad

import scala.collection._

import data._


trait Requester {
  def callUnit[T](setting: String, data: Data = Data.EMPTY): () => Unit =
    call(setting, data) { response => () }
  
  def call[T](setting: String, data: Data = Data.EMPTY)(unpacker: Data => T): () => T
}


abstract class ServerProxy(val name: String, val cxn: Connection) extends Requester {
  override def call[T](setting: String, data: Data)(unpacker: Data => T): () => T = {
    // need to do lookups
    val future = cxn.send(name) { setting -> data }
    () => unpacker(future()(0))
  }
  
  def newContext = cxn.newContext
}


class PacketProxy(server: ServerProxy, ctx: Context) extends Requester {
  private val records = mutable.Buffer.empty[(String, Data)]
  private var future: () => Seq[Data] = _
  
  override def call[T](setting: String, data: Data)(unpacker: Data => T): () => T = {
    val idx = records.size
    records += ((setting, data))
    () => unpacker(future()(idx))
  }
  
  def send: () => Unit = {
    future = server.cxn.send(server.name, ctx)(records: _*)
    () => { future(); () }
  }
}


trait ManagerServer extends Requester {
  def servers = call("Servers") { data =>
    for (Cluster(Word(id), Str(name)) <- data.getDataSeq) yield (id, name)
  }
  
  def settings(server: String) = call("Settings", Str(server)) { data =>
    for (Cluster(Word(id), Str(name)) <- data.getDataSeq) yield (id, name)
  }
  
  def lookupServer(name: String) = call("Lookup", Str(name)) { case Word(id) => id }
  
  def dataToString(data: Data) = call("Data To String", data) { case Str(s) => s }
  def stringToData(s: String) = call("String To Data", Str(s)) { data => data }
}

class ManagerServerProxy(name: String, cxn: Connection)
    extends ServerProxy(name, cxn) with ManagerServer {
  def packet(ctx: Context) = new ManagerServerPacket(this, ctx)
}

class ManagerServerPacket(server: ServerProxy, ctx: Context)
  extends PacketProxy(server, ctx) with ManagerServer
