package org.labrad.manager

import org.labrad.ServerInfo
import org.labrad.annotations._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.manager.auth._
import org.labrad.registry._
import org.labrad.types._
import org.labrad.util._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._

trait Hub {
  def allocateClientId(name: String): Long
  def allocateServerId(name: String): Long

  def getServerId(name: String): Long

  def connectClient(id: Long, name: String, handler: ClientActor): Unit
  def connectServer(id: Long, name: String, handler: ServerActor): Unit

  def username(id: Long): String
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

  def authServerConnected: Future[Unit]
  def registryConnected: Future[Unit]
}

object Hub {
  def notAServer(id: Long) = Future.failed(new Exception(s"target is not a server: $id"))
  def noSuchServer(id: Long) = Future.failed(new Exception(s"no such server: $id"))
  def notFound(id: Long) = Future.failed(new Exception(s"target not found: $id"))
}

class HubImpl(tracker: StatsTracker, _messager: () => Messager)(implicit ec: ExecutionContext)
extends Hub with Logging {

  private val allocatedClientIds = mutable.Map.empty[Long, String]
  private val allocatedServerIds = mutable.Map.empty[Long, String]

  private val handlerMap = mutable.Map.empty[Long, ClientActor]
  private def serverHandlers = handlerMap.values.collect { case s: ServerActor => s }

  private val serverIdCache = mutable.Map.empty[String, Long]
  private val serverInfoCache = mutable.Map.empty[Long, ServerInfo]

  private lazy val messager = _messager()

  private val serverCounter = new Counter(2L, Manager.ClientIdStart - 1) // id 1 is reserved
  private val clientCounter = new Counter(Manager.ClientIdStart, 0xFFFFFFFFL)

  private var nServersAllocated = 0L
  private var nServersConnected = 0L

  private var nClientsAllocated = 0L
  private var nClientsConnected = 0L

  private val _authServerConnected = Promise[Unit]
  private val _registryConnected = Promise[Unit]

  val authServerConnected = _authServerConnected.future
  val registryConnected = _registryConnected.future

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

  def getServerId(name: String): Long = synchronized {
    serverIdCache.get(name).getOrElse { sys.error(s"server does not exist: $name") }
  }

  def connectServer(id: Long, name: String, handler: ServerActor): Unit = {
    synchronized {
      require(allocatedServerIds.contains(id), s"cannot connect server with unallocated id: $id")
      require(allocatedServerIds(id) == name, s"id $id is not allocated to server $name")
      require(!handlerMap.contains(id), s"server already connected: $name ($id)")
      handlerMap(id) = handler
      tracker.connectServer(id, name)
      nServersConnected += 1
      name match {
        case Authenticator.NAME => _authServerConnected.trySuccess(())
        case Registry.NAME => _registryConnected.trySuccess(())
        case _ =>
      }
    }
    messager.broadcast(Manager.Connect(id, name, isServer = true), sourceId = Manager.ID)
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
    messager.broadcast(Manager.Connect(id, name, isServer = false), sourceId = Manager.ID)
  }

  def username(id: Long): String = {
    val handlerOpt = synchronized {
      handlerMap.get(id)
    }
    val handler = handlerOpt.getOrElse { sys.error(s"no such connection: $id") }
    handler.username
  }

  def close(id: Long): Unit = {
    val handlerOpt = synchronized {
      handlerMap.get(id)
    }
    handlerOpt.foreach(_.close())
  }

  def disconnect(id: Long): Unit = {
    val (name, isServer, allServers) = synchronized {
      val (handler, isServer) = handlerMap.get(id) match {
        case Some(handler: ServerActor) => (handler, true)
        case Some(handler)              => (handler, false)
        case None =>
          log.debug(s"disconnect called on unknown id: $id")
          return
      }

      // look up the name for this id
      val name = if (isServer) {
        allocatedServerIds(id)
      } else {
        allocatedClientIds(id)
      }

      log.debug(s"disconnecting id=$id (name=$name): $handler")

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

      // get a list of all connected servers that will be notified
      val allServers = this.serverHandlers

      (name, isServer, allServers)
    }

    // send expiration messages to all remaining servers (asynchronously)
    messager.broadcast(Manager.ExpireAll(id), sourceId = Manager.ID)
    Future.sequence(allServers.map(_.expireAll(id)(10.seconds))) onFailure {
      case ex => //log.warn("error while sending context expiration messages", ex)
    }

    // send server disconnect message
    if (isServer) {
      messager.broadcast(Manager.DisconnectServer(id, name), sourceId = Manager.ID)
    }
    messager.broadcast(Manager.Disconnect(id, name, isServer = true), sourceId = Manager.ID)
  }

  def message(id: Long, packet: Packet): Unit = {
    val handlerOpt = synchronized {
      handlerMap.get(id)
    }
    handlerOpt match {
      case Some(handler) => handler.message(packet)
      case None => log.debug(s"Message sent to non-existent target: $id")
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

  def expireContext(ctx: Context)(implicit timeout: Duration): Future[Long] = {
    val allServers = synchronized { this.serverHandlers }
    Future.sequence(allServers.map(_.expireContext(ctx))).map(_.sum)
  }

  def expireContext(id: Long, ctx: Context)(implicit timeout: Duration): Future[Long] = {
    val handlerOpt = synchronized { handlerMap.get(id) }
    handlerOpt match {
      case Some(s: ServerActor) => s.expireContext(ctx)(10.seconds)
      case Some(_) => Hub.notAServer(id)
      case None => Hub.noSuchServer(id)
    }
  }

  def expireAll(id: Long, high: Long)(implicit timeout: Duration): Future[Long] = {
    val handlerOpt = synchronized { handlerMap.get(id) }
    handlerOpt match {
      case Some(s: ServerActor) => s.expireAll(high)
      case Some(_) => Hub.notAServer(id)
      case None => Hub.noSuchServer(id)
    }
  }

  // expose server metadata
  def setServerInfo(info: ServerInfo): Unit = synchronized { serverInfoCache(info.id) = info }
  def serversInfo: Seq[ServerInfo] = synchronized { serverInfoCache.values.toVector }.sortBy(_.id)
  def serverInfo(id: Either[Long, String]): Option[ServerInfo] = synchronized {
    id.fold(i => serverInfoCache.get(i), n => serverInfoCache.values.find(_.name == n))
  }
}
