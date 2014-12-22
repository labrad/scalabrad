package org.labrad.manager

import org.jboss.netty.channel._
import org.labrad.ServerInfo
import org.labrad.annotations._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.registry._
import org.labrad.types._
import org.labrad.util._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

trait Hub {
  def allocateClientId(name: String): Long
  def allocateServerId(name: String): Long

  def connectClient(id: Long, name: String, handler: ClientActor): Unit
  def connectServer(id: Long, name: String, handler: ServerActor): Unit

  def close(id: Long): Unit
  def disconnect(id: Long): Unit

  def message(id: Long, packet: Packet): Unit
  def request(id: Long, packet: Packet)(implicit timeout: Duration): Future[Packet]

  def expireContext(ctx: Context)(implicit timeout: Duration): Future[Long]
  def expireContext(id: Long, ctx: Context)(implicit timeout: Duration): Future[Long]
  def expireAll(id: Long, high: Long)(implicit timeout: Duration): Future[Long]

  def setServerInfo(info: ServerInfo): Unit
  def serversInfo: Seq[ServerInfo]
  def serverInfo(id: Either[Long, String]): Option[ServerInfo]
}

object Hub {
  def notAServer(id: Long) = Future.failed(new Exception(s"target is not a server: $id"))
  def noSuchServer(id: Long) = Future.failed(new Exception(s"no such server: $id"))
  def notFound(id: Long) = Future.failed(new Exception(s"target not found: $id"))
}

class HubImpl(tracker: StatsTracker, _messager: () => Messager) extends Hub with Logging {

  private val allocatedClientIds = mutable.Map.empty[Long, String]
  private val allocatedServerIds = mutable.Map.empty[Long, String]

  private val handlerMap = mutable.Map.empty[Long, ClientActor]
  private def servers = handlerMap.values.collect { case s: ServerActor => s }

  private val serverIdCache = mutable.Map.empty[String, Long]
  private val serverInfoCache = mutable.Map.empty[Long, ServerInfo]

  private lazy val messager = _messager()

  private val serverCounter = new Counter(0x00000002L, 0x7FFFFFFFL) // 2 to 2**31-1. id 1 is reserved
  private val clientCounter = new Counter(0x80000000L, 0xFFFFFFFFL) // 2**31 to 2**32-1

  private var nServersAllocated = 0L
  private var nServersConnected = 0L

  private var nClientsAllocated = 0L
  private var nClientsConnected = 0L

  def allocateServerId(name: String): Long = synchronized {
    serverIdCache.get(name).getOrElse {
      require(nServersAllocated < serverCounter.length, "no server ids available")
      var id = serverCounter.next
      while (allocatedServerIds.contains(id)) id = serverCounter.next
      allocatedServerIds(id) = name
      serverIdCache(name) = id
      nServersAllocated += 1
      id
    }
  }

  def allocateClientId(name: String): Long = synchronized {
    require(nClientsAllocated < clientCounter.length, "no client ids available")
    var id = clientCounter.next
    while (allocatedClientIds.contains(id)) id = clientCounter.next
    allocatedClientIds(id) = name
    nClientsAllocated += 1
    id
  }

  def connectServer(id: Long, name: String, handler: ServerActor): Unit = {
    synchronized {
      require(allocatedServerIds.contains(id), s"cannot connect server with unallocated id: $id")
      require(allocatedServerIds(id) == name, s"id $id is not allocated to server $name")
      require(!handlerMap.contains(id), s"server already connected: $name ($id)")
      handlerMap(id) = handler
      tracker.connectServer(id, name)
      nServersConnected += 1
    }
    messager.broadcast("Connect", UInt(id), 1)
  }

  def connectClient(id: Long, name: String, handler: ClientActor): Unit = {
    synchronized {
      require(allocatedClientIds.contains(id), s"cannot connect client with unallocated id: $id")
      require(allocatedClientIds(id) == name, s"id $id is not allocated to client $name")
      require(!handlerMap.contains(id), s"client already connected: $name ($id)")
      handlerMap(id) = handler
      tracker.connectClient(id, name)
      nClientsConnected += 1
    }
    messager.broadcast("Connect", UInt(id), 1)
  }

  def close(id: Long): Unit = {
    val handlerOpt = synchronized {
      handlerMap.get(id)
    }
    handlerOpt.foreach(_.close())
  }

  def disconnect(id: Long): Unit = {
    val (serverNameOpt) = synchronized {
      val (handler, isServer) = handlerMap.get(id) match {
        case Some(handler: ServerActor) => (handler, true)
        case Some(handler)              => (handler, false)
        case None =>
          log.warn(s"disconnect called on unknown id: $id")
          return
      }

      log.debug(s"disconnecting (id=$id): $handler")

      // remove from the handler map
      handlerMap -= id
      tracker.disconnect(id)

      // decrement counters and remove cached metadata
      if (isServer) {
        nServersConnected -= 1
        serverInfoCache -= id
      } else {
        nClientsConnected -= 1
        nClientsAllocated -= 1
        allocatedClientIds -= id
      }

      if (isServer) allocatedServerIds.get(id) else None
    }

    // send expiration messages to all remaining servers (asynchronously)
    messager.broadcast("Expire All", UInt(id), 1)
    Future.sequence(servers.map(_.expireAll(id)(10.seconds))) onFailure {
      case ex => //log.warn("error while sending context expiration messages", ex)
    }

    // send server disconnect message
    for (name <- serverNameOpt) {
      messager.broadcast("Server Disconnect", Cluster(UInt(id), Str(name)), 1)
    }
    messager.broadcast("Disconnect", UInt(id), 1)
  }

  def message(id: Long, packet: Packet): Unit = {
    val handlerOpt = synchronized {
      handlerMap.get(id)
    }
    handlerOpt match {
      case Some(handler) => handler.message(packet)
      case None => log.warn("Message sent to non-existent target: " + id)
    }
  }

  def request(id: Long, packet: Packet)(implicit timeout: Duration): Future[Packet] = {
    val handlerOpt = synchronized {
      handlerMap.get(id)
    }
    handlerOpt match {
      case Some(s: ServerActor) => s.request(packet)
      case Some(_) => Hub.notAServer(id)
      case None => Hub.noSuchServer(id)
    }
  }

  def expireContext(ctx: Context)(implicit timeout: Duration): Future[Long] = synchronized {
    Future.sequence(servers.map(_.expireContext(ctx))).map(_.sum)
  }

  def expireContext(id: Long, ctx: Context)(implicit timeout: Duration): Future[Long] = synchronized {
    handlerMap.get(id) match {
      case Some(s: ServerActor) => s.expireContext(ctx)
      case Some(_) => Hub.notAServer(id)
      case None => Hub.noSuchServer(id)
    }
  }

  def expireAll(id: Long, high: Long)(implicit timeout: Duration): Future[Long] = synchronized {
    handlerMap.get(id) match {
      case Some(s: ServerActor) => s.expireAll(high)
      case Some(_) => Hub.notAServer(id)
      case None => Hub.noSuchServer(id)
    }
  }

  // expose server metadata
  def setServerInfo(info: ServerInfo): Unit = synchronized { serverInfoCache(info.id) = info }
  def serversInfo: Seq[ServerInfo] = synchronized { serverInfoCache.values.toSeq.sortBy(_.id) }
  def serverInfo(id: Either[Long, String]): Option[ServerInfo] = synchronized {
    id.fold(i => serverInfoCache.get(i), n => serverInfoCache.values.find(_.name == n))
  }
}
