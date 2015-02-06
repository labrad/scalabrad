package org.labrad.manager

import io.netty.channel._
import org.labrad.{ Reflect, RequestContext, Server, ServerInfo, SettingInfo }
import org.labrad.annotations._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.registry._
import org.labrad.types._
import org.labrad.util._
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait ClientActor {
  def message(packet: Packet): Unit

  def close(): Unit
}

trait ServerActor extends ClientActor {
  def message(packet: Packet): Unit
  def request(packet: Packet)(implicit timeout: Duration): Future[Packet]

  def expireContext(ctx: Context)(implicit timeout: Duration): Future[Long]
  def expireAll(high: Long)(implicit timeout: Duration): Future[Long]

  def close(): Unit
}

class ClientHandler(hub: Hub, tracker: StatsTracker, messager: Messager, channel: Channel, id: Long, name: String)(implicit ec: ExecutionContext)
extends SimpleChannelInboundHandler[Packet] with ClientActor with ManagerSupport with Logging {

  // handle incoming packets
  override def channelRead0(ctx: ChannelHandlerContext, packet: Packet): Unit = {
    packet match {
      case Packet(0, _, _, _)          => handleMessage(packet)
      case Packet(r, _, _, _) if r > 0 => handleRequest(packet)
      case Packet(r, _, _, _) if r < 0 => handleResponse(packet)
    }
  }

  protected def handleMessage(packet: Packet): Unit = {
    hub.message(packet.target, packet.copy(target=id))
  }

  protected def handleRequest(packet: Packet): Unit = {
    implicit val timeout = 10.minutes
    tracker.clientReq(id)
    (packet match {
      case Packet(_, 1, _, _) => handleManagerRequest(packet)
      case Packet(_, target, _, _) => hub.request(target, packet.copy(target=id))
    }) recover {
      case e: Throwable =>
        val err = Error(1, e.toString)
        packet.copy(id = -packet.id, records = Seq(Record(1, err)))
    } onComplete {
      case Success(response) =>
        tracker.clientRep(id)
        channel.writeAndFlush(response)
      case Failure(ex) =>
        log.error("error. this should not happen!", ex)
    }
  }

  protected def handleResponse(packet: Packet): Unit = {
    log.error(s"got response packet in ClientActor: $packet")
  }

  // handle outgoing packets
  override def message(packet: Packet): Unit = {
    log.debug(s"sending message (target=$id): $packet")
    channel.writeAndFlush(packet)
  }

  override def close(): Unit = channel.close()

  override def channelUnregistered(ctx: ChannelHandlerContext): Unit = {
    hub.disconnect(id)
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    hub.disconnect(id)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, ex: Throwable): Unit = {
    log.error("exceptionCaught", ex)
    ctx.close()
  }

  // handle manager calls locally
  protected val mgr = new ManagerImpl(id, name, hub, this, tracker, messager)
  protected val mgrHandler = ManagerImpl.bind(mgr)

  private def handleManagerRequest(packet: Packet): Future[Packet] = synchronized {
    tracker.serverReq(Manager.ID)
    val response = Server.handle(packet) { req => mgrHandler(req) }
    tracker.serverRep(Manager.ID)
    Future.successful(response)
  }

  // support methods for manager settings
  def startServing: Unit = ???

  def addSetting(id: Long, name: String, doc: String, accepts: String, returns: String): Unit = ???
  def delSetting(id: Long): Unit = ???
  def delSetting(name: String): Unit = ???

  def startNotifications(r: RequestContext, settingId: Long, expireAll: Boolean): Unit = ???
  def stopNotifications(r: RequestContext): Unit = ???
}


class ServerHandler(hub: Hub, tracker: StatsTracker, messager: Messager, channel: Channel, id: Long, name: String, doc: String)(implicit ec: ExecutionContext)
extends ClientHandler(hub, tracker, messager, channel, id, name) with ServerActor with ManagerSupport with Logging {

  private var contexts = Set.empty[Context]
  private var promises = Map.empty[Int, Promise[Packet]]

  override def request(packet: Packet)(implicit timeout: Duration): Future[Packet] = synchronized {
    tracker.serverReq(id)
    try {
      val converted = packet.records map { case Record(id, data) =>
        settingsById.get(id) match {
          case Some(setting) => Record(id, data.convertTo(setting.accepts))
          case None => sys.error(s"No setting with id $id")
        }
      }
      log.debug(s"sending packet: ${packet}...")
      val promise = Promise[Packet]
      promises += -packet.id -> promise
      contexts += packet.context
      channel.writeAndFlush(packet.copy(records = converted))
      promise.future.onComplete { _ => tracker.serverRep(id) }
      promise.future
    } catch {
      case ex: Throwable => Future.failed(ex)
    }
  }

  override protected def handleResponse(packet: Packet): Unit = synchronized {
    promises.get(packet.id) match {
      case Some(promise) =>
        log.debug(s"handle response: ${packet}")
        promises -= packet.id
        promise.success(packet.copy(target=id))
      case None =>
        log.error(s"invalid response with id ${packet.id}")
    }
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = synchronized {
    for ((_, promise) <- promises)
      promise.failure(LabradException(8, "Server disconnected"))

    promises = promises.empty
  }


  // support methods for manager settings
  private var settingsById: Map[Long, SettingInfo] = Map.empty
  private var settingsByName: Map[String, SettingInfo] = Map.empty
  private var contextExpirationInfo: Option[(Long, Boolean)] = None

  override def startServing: Unit =
    hub.setServerInfo(ServerInfo(id, name, doc, settingsById.values.toSeq))

  override def addSetting(id: Long, name: String, doc: String, accepts: String, returns: String): Unit = {
    require(!settingsById.contains(id), s"Setting already exists with id $id")
    require(!settingsByName.contains(name), s"Setting already exists with name '$name'")
    val inf = SettingInfo(id, name, doc, Pattern(accepts), Pattern(returns))
    settingsById += id -> inf
    settingsByName += name -> inf
  }

  override def delSetting(id: Long): Unit =
    settingsById.get(id).map(delSetting).getOrElse(sys.error(s"No setting with id $id"))

  override def delSetting(name: String): Unit =
    settingsByName.get(name).map(delSetting).getOrElse(sys.error(s"No setting '$name'"))

  private def delSetting(inf: SettingInfo): Unit = {
    settingsById -= inf.id
    settingsByName -= inf.name
  }

  override def startNotifications(r: RequestContext, settingId: Long, expireAll: Boolean): Unit = {
    mgr.subscribeToNamedMessage(r, "Expire Context", settingId, true)
    contextExpirationInfo = Some((settingId, expireAll))
  }

  override def stopNotifications(r: RequestContext): Unit = {
    contextExpirationInfo.foreach { case (settingId, expireAll) =>
      mgr.subscribeToNamedMessage(r, "Expire Context", settingId, false)
    }
    contextExpirationInfo = None
  }

  def expireContext(ctx: Context)(implicit timeout: Duration): Future[Long] = synchronized {
    val result = contextExpirationInfo match {
      case Some((settingId, _)) =>
        if (contexts contains ctx) {
          message(Packet(0, 1, ctx, Seq(Record(settingId, ctx.toData))))
          contexts -= ctx
          1L
        } else {
          0L
        }
      case None =>
        0L
    }
    Future.successful(result)
  }

  def expireAll(high: Long)(implicit timeout: Duration): Future[Long] = synchronized {
    val result = contextExpirationInfo match {
      case Some((settingId, expireAll)) =>
        val expired = contexts.filter(_.high == high)
        if (!expired.isEmpty) {
          if (expireAll) {
            message(Packet(0, 1, Context(high, 0), Seq(Record(settingId, UInt(high)))))
          } else {
            for (ctx <- expired)
              message(Packet(0, 1, ctx, Seq(Record(settingId, ctx.toData))))
          }
          contexts --= expired
          1L
        } else {
          0L
        }
      case None =>
        0L
    }
    Future.successful(result)
  }
}


/**
 * Internal methods that must be made available to the ManagerImpl
 */
trait ManagerSupport {
  def startServing(): Unit

  def addSetting(id: Long, name: String, doc: String, accepts: String, returns: String): Unit
  def delSetting(id: Long): Unit
  def delSetting(name: String): Unit

  def startNotifications(r: RequestContext, msgId: Long, expireAll: Boolean): Unit
  def stopNotifications(r: RequestContext): Unit
}


/**
 * Implementation of manager settings (server id 1) that provide various
 * administrative information and functions to all connected clients and servers.
 */
class ManagerImpl(id: Long, name: String, hub: Hub, stub: ManagerSupport, tracker: StatsTracker, messager: Messager) {
  private def serverInfo(id: Either[Long, String]): ServerInfo = id match {
    case Left(Manager.ID) | Right(Manager.NAME) => ManagerImpl.info
    case _ =>
      hub.serverInfo(id).getOrElse(sys.error("Server not found: " + id.fold(_.toString, _.toString)))
  }

  // metadata lookup and help

  @Setting(id=1, name="Servers", doc="Get a list of ids and names for all currently-connected servers.")
  def servers(r: RequestContext): Seq[(Long, String)] =
    (Manager.ID, Manager.NAME) +: hub.serversInfo.map(s => (s.id, s.name)).sorted

  @Setting(id=2, name="Settings", doc="Get a list of ids and names of settings for the given server, specified by name or id.")
  def settings(r: RequestContext, serverId: Either[Long, String]): Seq[(Long, String)] =
    serverInfo(serverId).settings.map(s => (s.id, s.name)).sorted

  @Setting(id=3, name="Lookup", doc="Lookup server or setting ids by name. If a single argument is given, returns the server id.")
  def lookup(r: RequestContext, name: String): Long = serverInfo(Right(name)).id
  def lookup(r: RequestContext,
             serverId: Either[Long, String],
             settingNames: Either[String, Seq[String]]): (Long, Either[Long, Seq[Long]]) = {
    val server = serverInfo(serverId)
    def toId(name: String): Long =
      server.setting(name).map(_.id).getOrElse(sys.error(s"Server '${server.name}' has no setting '$name'"))
    val settingIds = settingNames match {
      case Left(name) => Left(toId(name))
      case Right(names) => Right(names.map(toId))
    }
    (server.id, settingIds)
  }

  @Setting(id=10, name="Help", doc="Get help documentation for the given server or setting.")
  def help(r: RequestContext, serverId: Either[Long, String]): (String, String) = {
    val server = serverInfo(serverId)
    (server.doc, "") // TODO: get rid of notes field
  }
  def help(r: RequestContext, serverId: Either[Long, String], settingId: Either[Long, String]): (String, Seq[String], Seq[String], String) = {
    val server = serverInfo(serverId)
    settingId.fold(server.setting, server.setting) match {
      case None =>
        sys.error("Setting not found: " + settingId.fold(_.toString, _.toString))
      case Some(setting) =>
        (setting.doc,
         setting.accepts.expand.map(_.toString), // TODO: setting.accepts.toString
         setting.returns.expand.map(_.toString), // TODO: setting.returns.toString
         "" // TODO: get rid of notes field
        )
    }
  }

  // contexts and messages

  @Setting(id=50, name="Expire Context", doc="Expire the context in which this request was sent.")
  def expireContext(r: RequestContext, server: Option[Long]): Unit = {
    implicit val timeout = 1.minute

    // send messages to servers that have seen the given context
    val f = server match {
      case Some(id) => hub.expireContext(id, r.context)
      case None => hub.expireContext(r.context)
    }
    Await.result(f, timeout)

    // send a named message to anyone who cares
    messager.broadcast(Manager.ExpireContext(r.context), sourceId = id)
  }

  @Setting(id=51, name="Expire All", doc="Expire all contexts matching the high context of this request.")
  def expireAll(r: RequestContext): Unit = {
    implicit val timeout = 1.minute

    // send messages to servers that have had requests with the given high context
    Await.result(hub.expireContext(r.context), 1.minute)

    // send a named message to anyone who cares
    messager.broadcast(Manager.ExpireAll(r.context.high), sourceId = id)
  }

  @Setting(id=60, name="Subscribe to Named Message", doc="Register to receive messages identified by a particular name.")
  def subscribeToNamedMessage(r: RequestContext, name: String, msgId: Long, active: Boolean): Unit = {
    if (active) {
      messager.register(name, id, r.context, msgId)
    } else {
      messager.unregister(name, id, r.context, msgId)
    }
  }

  @Setting(id=61, name="Send Named Message", doc="Send a message with the given name, to be forward to all registered subscribers.")
  def sendNamedMessage(r: RequestContext, name: String, message: Data): Unit = {
    messager.broadcast(name, message, id)
  }

  // server settings (should stay local)

  @Setting(id=100, name="S: Register Setting", doc="(Servers only) Register an available setting for this server.") // TODO: change types to *(s, s) instead of *s, *s
  def addSetting(r: RequestContext, id: Long, name: String, doc: String,
                 accepted: Seq[String], returned: Seq[String], notes: String): Unit = {
    def makePattern(ps: Seq[String]) = ps match {
      case Seq()  => "?"
      case Seq(p) => p
      case ps     => Pattern.reduce(ps.map(Pattern(_)): _*).toString
    }
    val docStr = if (notes.isEmpty) doc else s"$doc\n\n$notes"
    stub.addSetting(id, name, docStr, makePattern(accepted), makePattern(returned))
  }

  @Setting(id=101, name="S: Unregister Setting", doc="(Servers only) Unregister an available setting for this server.")
  def delSetting(r: RequestContext, setting: Either[Long, String]): Unit = {
    setting.fold(stub.delSetting, stub.delSetting)
  }

  @Setting(id=110, name="S: Notify on Context Expiration", doc="(Servers only) Register to be notified when contexts which have been used to send requests to this server are expired.")
  def notifyOnContextExpiration(r: RequestContext, data: Option[(Long, Boolean)]): Unit = data match {
    case Some((msgId, expireAll)) => stub.startNotifications(r, msgId, expireAll)
    case None => stub.stopNotifications(r)
  }

  @Setting(id=120, name="S: Start Serving", doc="(Servers only) Signal that the server is ready to receive requests. Before this point, the server will not appear in the list of active servers nor any metadata lookups.")
  def startServing(r: RequestContext): Unit = {
    stub.startServing
    messager.broadcast(Manager.ConnectServer(id, name), sourceId = Manager.ID)
  }

  // utility methods (should stay local)

  @Setting(id=200, name="Data To String", doc="Convert data into its human-readable string representation.")
  def dataToString(r: RequestContext, data: Data): String = data.toString

  @Setting(id=201, name="String To Data", doc="Parse a string into labrad data.")
  def stringToData(r: RequestContext, str: String): Data = Data.parse(str)

  @Setting(id=1010, name="Convert", doc="Convert the given data to a new type.")
  def convert(r: RequestContext, data: Data, pattern: String): Data = Pattern(pattern)(data.t) match {
    case Some(t) => data.convertTo(t)
    case None => sys.error(s"cannot convert ${data.t} to $pattern")
  }

  @Setting(id=10000, name="Connection Info", doc="Get information about connected servers and clients.")
  def connectionInfo(r: RequestContext): Seq[(Long, String, Boolean, Long, Long, Long, Long, Long, Long)] = {
    val serverIds = hub.serversInfo.map(_.id).toSet
    for (s <- tracker.stats if !s.isServer || serverIds(s.id)) yield {
      val sReqs = s.sReqs - (if (s.id == Manager.ID) 1 else 0) // don't count this request for Manager
      val cReqs = s.cReqs - (if (s.id == id) 1 else 0) // don't count this request for client
      (s.id, s.name, s.isServer, sReqs, s.sReps, cReqs, s.cReps, s.msgSent, s.msgRecd)
    }
  }

  @Setting(id=14321, name="Close Connection", doc="Close a connection with the specified id.")
  def closeConnection(r: RequestContext, id: Long): Unit =
    hub.close(id)

  // TODO: other control stuff here, e.g. shutting down connections and even the entire service?

  @Setting(id=13579, name="Echo", doc="Echo back the supplied data.")
  def echo(r: RequestContext, data: Data): Data = data
}

object ManagerImpl {
  val (settings, bind) = Reflect.makeHandler[ManagerImpl]
  lazy val info = ServerInfo(Manager.ID, Manager.NAME, Manager.DOC, settings)
}
