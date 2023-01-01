package org.labrad.manager

import io.netty.channel._
import java.io.IOException
import org.labrad.{ Reflect, RequestContext, Server, ServerInfo, SettingInfo, TypeInfo }
import org.labrad.annotations._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.types._
import org.labrad.util._
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait ClientActor {
  def username: String

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

class ClientHandler(
  hub: Hub,
  tracker: StatsTracker,
  messager: Messager,
  channel: Channel,
  id: Long,
  name: String,
  val username: String
)
extends SimpleChannelInboundHandler[Packet] with ClientActor with ManagerSupport with Logging {

  // Handle scala Future callbacks on the global execution context.
  protected implicit def executionContext = ExecutionContext.global

  // handle incoming packets
  override def channelRead0(ctx: ChannelHandlerContext, packet: Packet): Unit = {
    if (packet.id > 0) {
      handleRequest(packet)
    } else if (packet.id == 0) {
      handleMessage(packet)
    } else {
      handleResponse(packet)
    }
  }

  protected def handleMessage(packet: Packet): Unit = {
    tracker.msgSend(id)
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
        e.printStackTrace()
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
    tracker.msgRecv(id)
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
    ex match {
      case _: IOException => // do nothing
      case e =>
        log.error("exceptionCaught", ex)
    }
    ctx.close()
  }

  // handle manager calls locally
  protected val mgr = new ManagerImpl(id, name, hub, this, tracker, messager)
  private val mgrHandler = ManagerImpl.bind(mgr)
  private val mgrLock = new Object

  private def handleManagerRequest(packet: Packet): Future[Packet] = {
    tracker.serverReq(Manager.ID)
    // use a lock to ensure that we process manager requests sequentially
    val response = mgrLock.synchronized {
      Server.handle(packet, includeStackTrace = false) { req =>
        mgrHandler(req)
      }
    }
    tracker.serverRep(Manager.ID)
    Future.successful(response)
  }

  // support methods for manager settings
  def startServing(): Unit = ???

  def addSetting(id: Long, name: String, doc: String, accepts: TypeInfo, returns: TypeInfo): Unit = ???
  def delSetting(id: Long): Unit = ???
  def delSetting(name: String): Unit = ???

  def startNotifications(r: RequestContext, settingId: Long, expireAll: Boolean): Unit = ???
  def stopNotifications(r: RequestContext): Unit = ???
}


class ServerHandler(
  hub: Hub,
  tracker: StatsTracker,
  messager: Messager,
  channel: Channel,
  id: Long,
  name: String,
  doc: String,
  username: String
)
extends ClientHandler(hub, tracker, messager, channel, id, name, username)
with ServerActor with ManagerSupport with Logging {

  private val contexts = mutable.Set.empty[Context]
  private val promises = mutable.Map.empty[(Long, Int), Promise[Packet]]

  private var settingsById: Map[Long, SettingInfo] = Map.empty
  private var settingsByName: Map[String, SettingInfo] = Map.empty
  private var contextExpirationInfo: Option[(Long, Context, Boolean)] = None


  override def request(packet: Packet)(implicit timeout: Duration): Future[Packet] = {
    tracker.serverReq(id)
    try {
      val (toSend, promise) = synchronized {
        val converted = packet.records map { case Record(id, data) =>
          settingsById.get(id) match {
            case Some(setting) => Record(id, data.convertTo(setting.accepts.pat))
            case None => sys.error(s"No setting with id $id")
          }
        }
        val promise = Promise[Packet]()
        val key = (packet.target, -packet.id)
        promises(key) = promise
        contexts += packet.context
        (packet.copy(records = converted), promise)
      }
      log.debug(s"sending packet: $toSend")
      channel.writeAndFlush(toSend)
      promise.future.onComplete { _ => tracker.serverRep(id) }
      promise.future
    } catch {
      case ex: Throwable => Future.failed(ex)
    }
  }

  override protected def handleResponse(packet: Packet): Unit = synchronized {
    val key = (packet.target, packet.id)
    promises.get(key) match {
      case Some(promise) =>
        log.debug(s"handle response: $packet")
        promises -= key
        promise.success(packet.copy(target=id))
      case None =>
        log.error(s"invalid response with id ${packet.id}. server=$id, name=$name, response=$packet")
    }
  }

  override def channelInactive(ctx: ChannelHandlerContext): Unit = synchronized {
    for ((_, promise) <- promises)
      promise.failure(LabradException(8, "Server disconnected"))

    promises.clear()
  }


  // support methods for manager settings
  override def startServing(): Unit = synchronized {
    hub.setServerInfo(ServerInfo(id, name, doc, settingsById.values.toSeq))
  }

  override def addSetting(id: Long, name: String, doc: String, accepts: TypeInfo, returns: TypeInfo): Unit = synchronized {
    require(!settingsById.contains(id), s"Setting already exists with id $id")
    require(!settingsByName.contains(name), s"Setting already exists with name '$name'")
    val inf = SettingInfo(id, name, doc, accepts, returns)
    settingsById += id -> inf
    settingsByName += name -> inf
  }

  override def delSetting(id: Long): Unit = synchronized {
    settingsById.get(id).map(delSetting).getOrElse(sys.error(s"No setting with id $id"))
  }

  override def delSetting(name: String): Unit = synchronized {
    settingsByName.get(name).map(delSetting).getOrElse(sys.error(s"No setting '$name'"))
  }

  private def delSetting(inf: SettingInfo): Unit = synchronized {
    settingsById -= inf.id
    settingsByName -= inf.name
  }

  override def startNotifications(r: RequestContext, settingId: Long, expireAll: Boolean): Unit = synchronized {
    contextExpirationInfo = Some((settingId, r.context, expireAll))
  }

  override def stopNotifications(r: RequestContext): Unit = synchronized {
    contextExpirationInfo = None
  }

  def expireContext(ctx: Context)(implicit timeout: Duration): Future[Long] = {
    var messages = Seq.newBuilder[Packet]
    val result = synchronized {
      contextExpirationInfo match {
        case Some((settingId, msgContext, _)) =>
          if (contexts contains ctx) {
            messages += Packet(0, 1, msgContext, Seq(Record(settingId, ctx.toData)))
            contexts -= ctx
            1L
          } else {
            0L
          }
        case None =>
          0L
      }
    }
    for (msg <- messages.result()) {
      message(msg)
    }
    Future.successful(result)
  }

  def expireAll(high: Long)(implicit timeout: Duration): Future[Long] = {
    val messages = Seq.newBuilder[Packet]
    val result = synchronized {
      contextExpirationInfo match {
        case Some((settingId, msgContext, expireAll)) =>
          val expired = contexts.filter(_.high == high)
          if (!expired.isEmpty) {
            if (expireAll) {
              messages += Packet(0, 1, msgContext, Seq(Record(settingId, UInt(high))))
            } else {
              for (ctx <- expired)
                messages += Packet(0, 1, msgContext, Seq(Record(settingId, ctx.toData)))
            }
            contexts --= expired
            1L
          } else {
            0L
          }
        case None =>
          0L
      }
    }
    for (msg <- messages.result()) {
      message(msg)
    }
    Future.successful(result)
  }
}


/**
 * Internal methods that must be made available to the ManagerImpl
 */
trait ManagerSupport {
  def username: String

  def startServing(): Unit

  def addSetting(id: Long, name: String, doc: String, accepts: TypeInfo, returns: TypeInfo): Unit
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

  @Setting(id = 1,
           name = "Servers",
           doc = """Get a list of ids and names for all currently-connected servers.""")
  def servers(): Seq[(Long, String)] =
    (Manager.ID, Manager.NAME) +: hub.serversInfo.map(s => (s.id, s.name)).sorted

  @Setting(id = 2,
           name = "Settings",
           doc = """Get a list of ids and names of settings for a server.
               |
               |The server can be specified by specified by string name or integer id.""")
  def settings(serverId: Either[Long, String]): Seq[(Long, String)] =
    serverInfo(serverId).settings.map(s => (s.id, s.name)).sorted

  @Setting(id = 3,
           name = "Lookup",
           doc = """Lookup server or setting ids by name.
               |
               |Can be called with a single string giving a server name, in which case we return the
               |server id. Or, can be called With two arguments, the server name or id, and one or
               |more setting names, in which case we return a cluster of server id and one or more
               |setting ids.""")
  def lookup(name: String): Long = serverInfo(Right(name)).id
  def lookup(serverId: Either[Long, String],
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

  @Setting(id = 10,
           name = "Help",
           doc = """Get help documentation for the given server or setting.
               |
               |If called with a single argument giving the server name or id, returns a cluster of
               |the server doc string and notes; the notes field however is deprecated, and all
               |information is contained in the first doc string.
               |
               |If called with two arguments giving the server id or name, and a setting id or name,
               |returns a cluster of (setting name, accepted types, return types, notes). Once
               |again, the notes field is deprecated and all information will be contained in the
               |first doc string.""")
  def help(serverId: Either[Long, String]): (String, String) = {
    val server = serverInfo(serverId)
    (server.doc, "") // TODO: get rid of notes field
  }
  def help(serverId: Either[Long, String], settingId: Either[Long, String]): (String, Seq[String], Seq[String], String) = {
    val server = serverInfo(serverId)
    settingId.fold(server.setting, server.setting) match {
      case None =>
        sys.error("Setting not found: " + settingId.fold(_.toString, _.toString))
      case Some(setting) =>
        (setting.doc,
         setting.accepts.strs, // TODO: setting.accepts.toString
         setting.returns.strs, // TODO: setting.returns.toString
         "" // TODO: get rid of notes field
        )
    }
  }

  @Setting(id = 20,
           name = "Version",
           doc = """Returns the manager version string.""")
  def version(): String = Manager.VERSION

  // contexts and messages

  @Setting(id = 50,
           name = "Expire Context",
           doc = """Expire the context in which this request was sent.""")
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

  @Setting(id = 51,
           name = "Expire All",
           doc = """Expire all contexts matching the high context of this request.""")
  def expireAll(r: RequestContext): Unit = {
    implicit val timeout = 1.minute

    // send messages to servers that have had requests with the given high context
    Await.result(hub.expireContext(r.context), 1.minute)

    // send a named message to anyone who cares
    messager.broadcast(Manager.ExpireAll(r.context.high), sourceId = id)
  }

  @Setting(id = 60,
           name = "Subscribe to Named Message",
           doc = """Register to receive messages identified by a particular name.
               |
               |Called with the message name, the id that should be used when sending messages with
               |this name, and a flag indicating whether to activate or deactivate sending this
               |named message to the given id.
               |
               |When a message is sent using the "Send Named Message" setting, it will will be
               |forwarded to subscribers in the context used when subscribing. The message will be
               |sent to the id given here, with the as the source, and with contents given as a
               |cluster of (sender_id, message), where senderId is the one who actually triggered
               |sending the named message.""")
  def subscribeToNamedMessage(r: RequestContext, name: String, msgId: Long, active: Boolean): Unit = {
    if (active) {
      messager.register(name, id, r.context, msgId)
    } else {
      messager.unregister(name, id, r.context, msgId)
    }
  }

  @Setting(id = 61,
           name = "Send Named Message",
           doc = """Send a message with the given name to all registered subscribers.
               |
               |For compatibility with the delphi manager, we actualy send to subscribers a cluster
               |of (sender_id, message), where sender_id is the id of the caller and message is the
               |provided message data.""")
  def sendNamedMessage(name: String, message: Data): Unit = {
    // For compatibility with delphi manager, we send a cluster of
    // id and message data as the message itself, and set the source
    // to always be the manager.
    messager.broadcast(name, Cluster(UInt(id), message), sourceId = Manager.ID)
  }

  // server settings (should stay local)

  @Setting(id = 100,
           name = "S: Register Setting",
           doc = """(Servers only) Register an available setting for this server.
               |
               |Setting info is given as (id, name, doc, accepted types, returned types, notes).
               |The final notes field is deprecated, and any information provided there will simply
               |be concatenated with the doc string.""")
  def addSetting(id: Long, name: String, doc: String,
                 accepted: Seq[String], returned: Seq[String], notes: String): Unit = {
    val docStr = if (notes.isEmpty) doc else s"$doc\n\n$notes"
    stub.addSetting(id, name, docStr, TypeInfo.fromPatterns(accepted), TypeInfo.fromPatterns(returned))
  }

  @Setting(id = 101,
           name = "S: Unregister Setting",
           doc = """(Servers only) Unregister an available setting for this server.
               |
               |The setting to unregister can be specified by name or id.""")
  def delSetting(setting: Either[Long, String]): Unit = {
    setting.fold(stub.delSetting, stub.delSetting)
  }

  @Setting(id = 110,
           name = "S: Notify on Context Expiration",
           doc = """(Servers only) Register to receive context expiration notifications.
               |
               |Can be called with two arguments specifying the message id to use for context
               |expiration notifications and a flag indicating whether to send combined "expire all"
               |messages. Or can be called with no arguments to disable sending context expiration
               |messages.
               |
               |Context expiration messages will be sent to the given message id in the context in
               |which this setting was called, for all contexts which have been used to make
               |requests to this server. Expiration can be triggered either by someone explicitly
               |calling one of the settings "Expire Context" or "Expire All", or by the connection
               |with matching high id being closed. In the case of an individual context expiration,
               |the message data will be a context id, given as a cluster of (high word, low word).
               |If the expire all flag is false, then when a connection closes or "Expire All" is
               |called the manager will also send out expiration messages with a context id cluster
               |for all associated contexts. However, if the expire all flag was true, then we
               |instead will send one single message containing just the high word of the context
               |(which matches the connection id).""")
  def notifyOnContextExpiration(r: RequestContext, data: Option[(Long, Boolean)]): Unit = data match {
    case Some((msgId, expireAll)) => stub.startNotifications(r, msgId, expireAll)
    case None => stub.stopNotifications(r)
  }

  @Setting(id = 120,
           name = "S: Start Serving",
           doc = """(Servers only) Signal that the server is ready to receive requests.
               |
               |Before calling this setting, the server will not appear in the list of active
               |servers nor any metadata lookups.""")
  def startServing(): Unit = {
    stub.startServing()
    messager.broadcast(Manager.ConnectServer(id, name), sourceId = Manager.ID)
  }

  // utility methods (should stay local)

  @Setting(id = 200,
           name = "Data To String",
           doc = """Convert data into its human-readable string representation.
               |
               |String representation can be parsed and converted back to data by calling the
               |"String To Data" setting.
               |
               |The string representation is subject to change and should not be used for long-term
               |storage of labrad data. Instead, it is recommended to use the binary format as sent
               |over the wire for long-term storage.""")
  def dataToString(data: Data): String = data.toString

  @Setting(id = 202,
           name = "Data To Pretty String",
           doc = """Convert data into a pretty-printed human-readable string representation.
               |
               |Takes an integer "width" and arbitrary data, and attempts to pretty-print the data
               |to be no more that "width" columns wide. The pretty-printed is not _guaranteed_ to
               |fit within the specified width since some things like string literals may be longer,
               |but the pretty-printer will try to make things fit.
               |
               |String representation can be parsed and converted back to data by calling the
               |"String To Data" setting.
               |
               |The string representation is subject to change and should not be used for long-term
               |storage of labrad data. Instead, it is recommended to use the binary format as sent
               |over the wire for long-term storage.""")
  def dataToPrettyString(width: Int, data: Data): String = data.toPrettyString(width)

  @Setting(id = 201,
           name = "String To Data",
           doc = """Parse a string from human-readable representation into labrad data.
               |
               |Can parse data in the string format as returned by the "Data To String" setting.
               |
               |The string representation is subject to change and should not be used for long-term
               |storage of labrad data. Instead, it is recommended to use the binary format as sent
               |over the wire for long-term storage.""")
  def stringToData(str: String): Data = Data.parse(str)

  @Setting(id = 1010,
           name = "Convert",
           doc = """Convert the given data to a new type.
               |
               |Called with data and a type tag string, will convert the data to the given type and
               |return it, or fail if this is not possible. This can be used to do unit conversion,
               |for example.""")
  def convert(data: Data, pattern: String): Data = Pattern(pattern)(data.t) match {
    case Some(t) => data.convertTo(t)
    case None => sys.error(s"cannot convert ${data.t} to $pattern")
  }

  @Setting(id = 10000,
           name = "Connection Info",
           doc = """Get information about connected servers and clients.
               |
               |Returns a list of clusters, one for each connection. Each cluster contains the
               |following elements:
               | - id: The connection id; pass to `Close Connection` to kill connection.
               | - name: The connection name.
               | - is_server: Boolean indicating if this is a server (true) or client (false).
               | - server_requests: Number of requests received by server (meaningless for clients).
               | - server_replies: Number of replies sent by server (meaningless for clients).
               | - client_requests: Number of requests sent by client or server.
               | - client_replies: Number of replies received by client or server.
               | - messages_sent: Number of messages sent by client or server.
               | - messages_received: Number of messages received by client or server.""")
  def connectionInfo(): Seq[(Long, String, Boolean, Long, Long, Long, Long, Long, Long)] = {
    val serverIds = (Manager.ID +: hub.serversInfo.map(_.id)).toSet
    for (s <- tracker.stats if !s.isServer || serverIds(s.id)) yield {
      val sReqs = s.sReqs - (if (s.id == Manager.ID) 1 else 0) // don't count this request for Manager
      val cReqs = s.cReqs - (if (s.id == id) 1 else 0) // don't count this request for client
      (s.id, s.name, s.isServer, sReqs, s.sReps, cReqs, s.cReps, s.msgSent, s.msgRecd)
    }
  }

  @Setting(id = 10001,
           name = "Connection Username",
           doc = """Get the username for the connection with the given id.
               |
               |If no id is given, get the username for the current connection.""")
  def connectionUsername(id: Option[Long]): String = {
    id match {
      case None => stub.username
      case Some(id) => hub.username(id)
    }
  }

  @Setting(id = 14321,
           name = "Close Connection",
           doc = """Close the connection with the specified id.""")
  def closeConnection(id: Long): Unit =
    hub.close(id)

  @Setting(id = 13579,
           name = "Echo",
           doc = """Echo back the supplied data.""")
  def echo(data: Data): Data = data
}

object ManagerImpl {
  val (settings, bind) = Reflect.makeHandler[ManagerImpl]
  lazy val info = ServerInfo(Manager.ID, Manager.NAME, Manager.DOC, settings)
}
