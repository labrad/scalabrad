package org.labrad.manager

import akka.actor.ActorRef
import akka.util.Timeout
import org.jboss.netty.channel._
import org.labrad.ServerInfo
import org.labrad.annotations._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.registry._
import org.labrad.types._
import org.labrad.util._
import org.labrad.util.akka.{TypedProps, TypedActor}
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

trait Hub {
  def allocateClientId(name: String): Long
  def allocateServerId(name: String): Long

  def connectClient(id: Long, name: String, handler: ClientActor): Boolean
  def connectServer(id: Long, name: String, handler: ServerActor): Boolean
  def connectClientRem(id: Long, name: String, hubActor: ActorRef): Boolean
  def connectServerRem(id: Long, name: String, hubActor: ActorRef): Boolean

  def close(id: Long): Unit
  def disconnect(id: Long): Unit

  def message(id: Long, packet: Packet): Unit
  def request(id: Long, packet: Packet)(implicit timeout: Timeout): Future[Packet]

  def expireContext(ctx: Context)(implicit timeout: Timeout): Future[Long]
  def expireContext(id: Long, ctx: Context)(implicit timeout: Timeout): Future[Long]
  def expireAll(id: Long, high: Long)(implicit timeout: Timeout): Future[Long]

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

  import TypedActor.dispatcher

  private val serverCounter = new Counter(0x00000002L, 0x7FFFFFFFL) // 2 to 2**31-1. id 1 is reserved
  private val clientCounter = new Counter(0x80000000L, 0xFFFFFFFFL) // 2**31 to 2**32-1

  private var nServersAllocated = 0L
  private var nServersConnected = 0L

  private var nClientsAllocated = 0L
  private var nClientsConnected = 0L

  def allocateServerId(name: String): Long = serverIdCache.get(name).getOrElse {
    require(nServersAllocated < serverCounter.length, "no server ids available")
    var id = serverCounter.next
    while (allocatedServerIds.contains(id)) id = serverCounter.next
    allocatedServerIds(id) = name
    serverIdCache(name) = id
    nServersAllocated += 1
    id
  }

  def allocateClientId(name: String): Long = {
    require(nClientsAllocated < clientCounter.length, "no client ids available")
    var id = clientCounter.next
    while (allocatedClientIds.contains(id)) id = clientCounter.next
    allocatedClientIds(id) = name
    nClientsAllocated += 1
    id
  }

  def connectServer(id: Long, name: String, handler: ServerActor): Boolean = {
    require(allocatedServerIds.contains(id), s"cannot connect server with unallocated id: $id")
    require(allocatedServerIds(id) == name, s"id $id is not allocated to server $name")
    require(!handlerMap.contains(id), s"server already connected: $name ($id)")
    handlerMap(id) = handler
    tracker.connectServer(id, name)
    nServersConnected += 1
    messager.broadcast("Connect", UInt(id), 1)
    true
  }

  def connectClient(id: Long, name: String, handler: ClientActor): Boolean = {
    require(allocatedClientIds.contains(id), s"cannot connect client with unallocated id: $id")
    require(allocatedClientIds(id) == name, s"id $id is not allocated to client $name")
    require(!handlerMap.contains(id), s"client already connected: $name ($id)")
    handlerMap(id) = handler
    tracker.connectClient(id, name)
    nClientsConnected += 1
    messager.broadcast("Connect", UInt(id), 1)
    true
  }

  def close(id: Long): Unit = handlerMap.get(id).foreach(_.close)

  private def hubFor(actor: ActorRef): Hub = {
    TypedActor(TypedActor.context.system).typedActorOf(TypedProps[Hub], actor)
  }

  def connectServerRem(id: Long, name: String, hubActor: ActorRef): Boolean = {
    val hub = hubFor(hubActor)
    val server = new ServerActor {
      def message(packet: Packet) { hub.message(id, packet) }
      def request(packet: Packet)(implicit timeout: Timeout) = hub.request(id, packet)

      def expireContext(ctx: Context)(implicit timeout: Timeout) = hub.expireContext(id, ctx)
      def expireAll(high: Long)(implicit timeout: Timeout) = hub.expireAll(id, high)

      def close(): Unit = hub.close(id)
    }
    connectServer(id, name, server)
  }

  def connectClientRem(id: Long, name: String, hubActor: ActorRef): Boolean = {
    val hub = hubFor(hubActor)
    val client = new ClientActor {
      def message(packet: Packet) { hub.message(id, packet) }

      def close(): Unit = hub.close(id)
    }
    connectClient(id, name, client)
  }

  def disconnect(id: Long): Unit = {
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

    // send expiration messages to all remaining servers (asynchronously)
    messager.broadcast("Expire All", UInt(id), 1)
    Future.sequence(servers.map(_.expireAll(id)(Timeout(10.seconds)))) onFailure {
      case ex => //log.warn("error while sending context expiration messages", ex)
    }

    // send server disconnect message
    if (isServer) {
      val name = allocatedServerIds(id)
      messager.broadcast("Server Disconnect", Cluster(UInt(id), Str(name)), 1)
    }
    messager.broadcast("Disconnect", UInt(id), 1)
  }

  def message(id: Long, packet: Packet): Unit = handlerMap.get(id) match {
    case Some(handler) => handler.message(packet)
    case None => log.warn("Message sent to non-existent target: " + id)
  }

  def request(id: Long, packet: Packet)(implicit timeout: Timeout): Future[Packet] = handlerMap.get(id) match {
    case Some(s: ServerActor) => s.request(packet)
    case Some(_) => Hub.notAServer(id)
    case None => Hub.noSuchServer(id)
  }

  def expireContext(ctx: Context)(implicit timeout: Timeout): Future[Long] =
    Future.sequence(servers.map(_.expireContext(ctx))).map(_.sum)

  def expireContext(id: Long, ctx: Context)(implicit timeout: Timeout): Future[Long] = handlerMap.get(id) match {
    case Some(s: ServerActor) => s.expireContext(ctx)
    case Some(_) => Hub.notAServer(id)
    case None => Hub.noSuchServer(id)
  }

  def expireAll(id: Long, high: Long)(implicit timeout: Timeout): Future[Long] = handlerMap.get(id) match {
    case Some(s: ServerActor) => s.expireAll(high)
    case Some(_) => Hub.notAServer(id)
    case None => Hub.noSuchServer(id)
  }

  // expose server metadata
  def setServerInfo(info: ServerInfo): Unit = { serverInfoCache(info.id) = info }
  def serversInfo = serverInfoCache.values.toSeq.sortBy(_.id)
  def serverInfo(id: Either[Long, String]): Option[ServerInfo] =
    id.fold(i => serverInfoCache.get(i), n => serverInfoCache.values.find(_.name == n))
}


class RemoteHubImpl(hub: Hub, tracker: StatsTracker) extends Hub with Logging {

  private val handlerMap = mutable.Map.empty[Long, ClientActor]

  def allocateServerId(name: String): Long = hub.allocateServerId(name)
  def allocateClientId(name: String): Long = hub.allocateClientId(name)

  private def thisRef = TypedActor(TypedActor.context.system).getActorRefFor(TypedActor.self)

  def connectServer(id: Long, name: String, handler: ServerActor): Boolean = {
    val result = hub.connectServerRem(id, name, thisRef)
    handlerMap(id) = handler
    result
  }

  def connectClient(id: Long, name: String, handler: ClientActor): Boolean = {
    val result = hub.connectClientRem(id, name, thisRef)
    handlerMap(id) = handler
    result
  }

  def connectServerRem(id: Long, name: String, handler: ActorRef): Boolean = sys.error("not implemented")
  def connectClientRem(id: Long, name: String, handler: ActorRef): Boolean = sys.error("not implemented")

  def disconnect(id: Long): Unit = {
    hub.disconnect(id)
    handlerMap -= id
  }

  def message(id: Long, packet: Packet): Unit = handlerMap.get(id) match {
    case Some(handler) => handler.message(packet)
    case None => hub.message(id, packet)
  }

  def request(id: Long, packet: Packet)(implicit timeout: Timeout): Future[Packet] = handlerMap.get(id) match {
    case Some(s: ServerActor) => s.request(packet)
    case Some(_) => Hub.notAServer(id)
    case None => hub.request(id, packet)
  }

  def expireContext(ctx: Context)(implicit timeout: Timeout): Future[Long] = hub.expireContext(ctx)

  def expireContext(id: Long, ctx: Context)(implicit timeout: Timeout): Future[Long] = handlerMap.get(id) match {
    case Some(s: ServerActor) => s.expireContext(ctx)
    case Some(_) => Hub.notAServer(id)
    case _ => hub.expireContext(id, ctx)
  }

  def expireAll(id: Long, high: Long)(implicit timeout: Timeout): Future[Long] = handlerMap.get(id) match {
    case Some(s: ServerActor) => s.expireAll(high)
    case Some(_) => Hub.notAServer(id)
    case _ => hub.expireAll(id, high)
  }

  def close(id: Long): Unit = handlerMap.get(id) match {
    case Some(handler) => handler.close()
    case None => hub.close(id)
  }

  // expose server metadata
  def setServerInfo(info: ServerInfo): Unit = hub.setServerInfo(info)
  def serversInfo = hub.serversInfo
  def serverInfo(id: Either[Long, String]): Option[ServerInfo] = hub.serverInfo(id)
}
