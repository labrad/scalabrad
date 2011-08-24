package org.labrad

import java.lang.reflect.InvocationTargetException
import java.net.InetSocketAddress
import java.security.MessageDigest
import java.util.concurrent.Executors

import scala.actors.Actor
import scala.actors.Actor._
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.Lock
import scala.util.Random
import scala.util.control.Breaks._

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers, HeapChannelBufferFactory}
import org.jboss.netty.channel._
import org.jboss.netty.channel.group._
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.handler.codec.oneone.{OneToOneDecoder, OneToOneEncoder}
import org.jboss.netty.handler.execution._

import akka.actor.TypedActor
import akka.dispatch.Future
import grizzled.slf4j.Logging

import annotations._
import data._
import errors._
import types._
import util._


@ChannelHandler.Sharable
class ChannelGrouper(group: ChannelGroup) extends SimpleChannelHandler with DisconnectOnError {
  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    group.add(e.getChannel)
  }
}


trait DisconnectOnError extends SimpleChannelHandler {
  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    e.getCause.printStackTrace
    e.getChannel.close
  }
}


class LoginHandler(auth: AuthService, hub: HandlerHub) extends SimpleChannelHandler with DisconnectOnError {
  
  private var channel: Channel = _
  
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    channel = e.getChannel
  }
  
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val packet @ Packet(request, target, context, records) = e.getMessage
    val data = (loginActor !? packet) match {
      case data: Data => data
      case (data: Data, id: Long, handler: ChannelHandler) =>
        finishLogin(ctx, id, handler)
        data
      case _ => Error(5, "Error while logging in.")
    }
    val response = Packet(-request, target, context, Seq(Record(0, data)))
    val future = e.getChannel.write(response)
    if (data.isError) future.addListener(ChannelFutureListener.CLOSE)
  }
  
  private val loginActor = actor {
    try {
      val challenge = Array.ofDim[Byte](256)
      Random.nextBytes(challenge)
      
      // login packet
      receive {
        case Packet(request, 1, _, Seq()) if request > 0 =>
          reply(Bytes(challenge))
        case _ =>
          throw new LabradException(1, "Invalid login packet")
      }
      
      // response to password challenge
      receive {
        case Packet(request, 1, _, Seq(Record(0, Bytes(response)))) if request > 0 =>
          if (auth.authenticate(challenge, response))
            reply(Str("LabRAD 2.0"))
          else
            throw new LabradException(2, "Incorrect password")
        case _ =>
          throw new LabradException(1, "Invalid authentication packet")
      }
      
      // identification as server or client
      receive {
        case Packet(request, 1, _, Seq(Record(0, data))) if request > 0 =>
          val handler = data match {
            case Cluster(Word(protocol), Str(name)) =>
              hub.connectClient(channel, name)
              
            case Cluster(Word(protocol), Str(name), Str(doc)) =>
              hub.connectServer(channel, name, doc)
              
            case Cluster(Word(protocol), Str(name), Str(doc), Str(remarks)) =>
              hub.connectServer(channel, name, doc + "\n\n" + remarks)
              
            case _ =>
              throw new LabradException(1, "Invalid identification packet")
          }
          reply((Word(handler.id), handler.id, handler))
        
        case _ =>
          throw new LabradException(1, "Invalid identification packet")
      }
    } catch {
      case ex: LabradException =>
        reply(ex.toData)
      case ex =>
        reply(Error(1, ex.toString))
    }
  }
  
  def finishLogin(ctx: ChannelHandlerContext, id: Long, handler: ChannelHandler) {
    val pipeline = ctx.getPipeline
    pipeline.addLast("contextDecoder", new ContextDecoder(id))
    pipeline.addLast("contextEncoder", new ContextEncoder(id))
    pipeline.addLast(handler.getClass.toString, handler)
    pipeline.remove(this)
  }
}


trait ResponseHandler {
  def sendResponse(packet: Packet): Unit
}

trait ClientHandler extends ResponseHandler {
  def id: Long
  def name: String
  def sendMessage(packet: Packet): Unit
  def sendRequest(packet: Packet, handler: ResponseHandler): Unit
}

trait ServerHandler extends ClientHandler {
  def doc: String
  
  def isServing: Boolean
  def startServing
  
  def settings: Seq[(Long, String)]
  def setting(id: Long): Option[SettingInfo]
  def setting(name: String): Option[SettingInfo]
  def setting(id: Either[Long, String]): Option[SettingInfo] = id match {
    case Left(id) => setting(id)
    case Right(name) => setting(name)
  }
  
  def registerSetting(id: Long, name: String, doc: String, accepts: Seq[String], returns: Seq[String]): Unit
  def unregisterSetting(id: Long): Unit
  def unregisterSetting(name: String): Unit
}


class ClientHandlerImpl(hub: HandlerHub, channel: Channel, val id: Long, val name: String)
    extends SimpleChannelHandler
    with ClientHandler
    with Logging {
  
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val packet @ Packet(request, target, context, records) = e.getMessage
    val result = try {
      packet match {
        case packet @ Packet(0, _, _, _) => handleMessage(packet)
        case packet @ Packet(r, _, _, _) if r > 0 => handleRequest(packet)
        case packet @ Packet(r, _, _, _) if r < 0 => handleResponse(packet)
        case _ => throw new LabradException(4, "Invalid message received by ClientHandler")
      }
      None
    } catch {
      case ex: LabradException => Some(ex.toData)
      case ex => Some(Error(1, ex.toString))
    }
    result map { error =>
      val id = if (records.isEmpty) 0 else records(0).id
      val resp = Seq(Record(id, error))
      e.getChannel.write(Packet(-request, target, context, resp))
    }
  }
  
  // the handle* methods are for incoming packets from this server/client, to be sent elsewhere
  protected def handleMessage(packet: Packet) {
    hub.getHandler(packet.target) match {
      case Some(handler) =>
        handler.sendMessage(packet.copy(target=id))
      case None =>
        error("Message target %d does not exits".format(packet.target))
    }
  }
  
  protected def handleRequest(packet: Packet) {
    hub.getHandler(packet.target) match {
      case Some(handler) =>
        handler.sendRequest(packet.copy(target=id), this)
      case None =>
        throw new LabradException(5, "Unknown target: " + packet.target)
    }
  }
  
  protected def handleResponse(packet: Packet) {
    error("got response packet in ClientHandler: " + packet)
  }
  
  // the send* methods are for outgoing packets from other servers/clients
  def sendMessage(packet: Packet) {
    channel.write(packet)
  }
  
  def sendRequest(packet: Packet, handler: ResponseHandler) {
    throw new LabradException(6, "Cannot send request to a client connection")
  }
  
  def sendResponse(packet: Packet) {
    debug("sending response (target=%d): %s...".format(id, packet))
    channel.write(packet)
  }
  
  
  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    // when we actually disconnect from the remote peer (happens before close)
  }
  
  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    // todo: keep track of pending requests and only disconnect when they are all complete
    hub.disconnect(this)
  }
}


class ServerHandlerImpl(hub: HandlerHub, channel: Channel, id: Long, name: String, val doc: String)
    extends ClientHandlerImpl(hub, channel, id, name)
    with ServerHandler
    with Logging {
  
  private var _serving = false
  def isServing = _serving
  def startServing = { _serving = true }
  
  private var contexts = Set.empty[Context]
  private var requestMap = Map.empty[Int, (Context, ResponseHandler)]
  
  override def sendRequest(packet: Packet, handler: ResponseHandler) {
    def checkRecord(id: Long, data: Data): Option[Data] = setting(id) match {
      case None => Some(Error(1, "No setting with id %d".format(id)))
      case Some(inf) =>
        if (inf.accepted exists { td => td.accepts(data.t) })
          None
        else
          Some(Error(1, "Setting %d does not accept data of type %s".format(id, data.t)))
    }
    for (Record(id, data) <- packet.records) {
      checkRecord(id, data) match {
        case Some(error) =>
          handler.sendResponse(packet.copy(id = -packet.id, records = Seq(Record(id, error))))
          return
        case None =>
      }
    }
    // TODO: handle unit conversion for compatible units
    debug("sending packet: " + packet + "...")
    requestMap += -packet.id -> (packet.context, handler)
    contexts += packet.context
    channel.write(packet)
  }
  
  protected override def handleResponse(packet: Packet) {
    requestMap.get(packet.id) match {
      case Some((context, handler)) =>
        debug("handle response: " + packet)
        requestMap -= packet.id
        handler.sendResponse(packet.copy(target=id))
      case None =>
        error("Target for response packet does not exist")
    }
  }
  
  override def channelDisconnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    for ((req, (context, handler)) <- requestMap) {
      val err = Error(8, "Server disconnected")
      handler.sendResponse(Packet(req, id, context, Seq(Record(1, err))))
    }
  }
  
  // stored information about settings
  private var settingsById: Map[Long, SettingInfo] = Map.empty
  private var settingsByName: Map[String, SettingInfo] = Map.empty
  
  def settings: Seq[(Long, String)] = settingsById.values.map(s => (s.id, s.name)).toSeq.sorted
  def setting(id: Long): Option[SettingInfo] = settingsById.get(id)
  def setting(name: String): Option[SettingInfo] = settingsByName.get(name)
  
  def registerSetting(
      id: Long,
      name: String,
      doc: String,
      accepts: Seq[String],
      returns: Seq[String]): Unit = synchronized {
    require(!settingsById.containsKey(id), "Setting with id %d already exists".format(id))
    require(!settingsByName.containsKey(name), "Setting with name '%s' already exists".format(name))
    val accepted = accepts map { t => TypeDescriptor(t) }
    val returned = returns map { t => TypeDescriptor(t) }
    val inf = SettingInfo(id, name, doc, accepted, returned)
    settingsById += id -> inf
    settingsByName += name -> inf
  }
  
  def unregisterSetting(id: Long): Unit = synchronized {
    settingsById.get(id) match {
      case None => throw new Exception("Setting with id %d does not exist".format(id))
      case Some(inf) => unregister(inf)
    }
  }
  
  def unregisterSetting(name: String): Unit = synchronized {
    settingsByName.get(name) match {
      case None => throw new Exception("Setting with name '%s' does not exist".format(name))
      case Some(inf) => unregister(inf)
    }
  }
  
  private def unregister(inf: SettingInfo) {
    settingsById -= inf.id
    settingsByName -= inf.name
  }
}


class ManagerHandler(val id: Long, val name: String, mgr: ManagerSettings) extends ServerHandler {
  val doc = "the Manager"
  
  def isServing = true
  def startServing: Unit = {}
  
  def sendMessage(packet: Packet) {}
  def sendResponse(packet: Packet) {}
  def sendRequest(packet: Packet, handler: ResponseHandler) {
    val source = handler.asInstanceOf[ClientHandler]
    mgr.serve(packet, source) map { handler.sendResponse(_) }
  }
  
  private val dispatchTable = ServerConnection.locateSettings(classOf[ManagerSettingsImpl])
  
  def settings: Seq[(Long, String)] =
    dispatchTable.toSeq map { case (id, setting) => (id, setting.info.name) } sorted
    
  def setting(id: Long) = dispatchTable.get(id) map toInfo
  
  def setting(name: String) = dispatchTable.values find { _.info.name == name } map toInfo
  
  private def toInfo(s: SettingHandler) =
    SettingInfo(s.info.id, s.info.name, s.info.doc, s.accepts, s.returns)
    
  def registerSetting(id: Long, name: String, doc: String, accepts: Seq[String], returns: Seq[String]): Unit = {}
  def unregisterSetting(id: Long): Unit = {}
  def unregisterSetting(name: String): Unit = {}
}


trait ManagerSettings {
  def serve(packet: Packet, source: ClientHandler): Future[Packet]
}


class ManagerSettingsImpl(hub: HandlerHub, messager: Messager) extends TypedActor with ManagerSettings with Logging {
  var ctx: Context = _
  var source: ClientHandler = _
  
  def serve(packet: Packet, source: ClientHandler): Future[Packet] = {
    val Packet(request, target, context, records) = packet
    this.ctx = context
    this.source = source
    val response = Seq.newBuilder[Record]
    breakable {
      for (Record(id, data) <- records) {
        val resp = try {
          serve(id, data) 
        } catch {
          case ex: LabradException => ex.toData
          case ex =>
            error(ex)
            Error(1, ex.toString)
        }
        response += Record(id, resp)
        if (resp.isError) break
      }
    }
    future(Packet(-request, target, context, response.result))
  }
  
  private def serve(id: Long, data: Data): Data = dispatchTable.get(id) match {
    case Some(handler) =>
      if (handler isDefinedAt data)
        handler(this, data)
      else
        Error(2, "Setting %d does not accept type: %s.  Accepted types are [%s].".format(id, data.t, handler.accepts.mkString(", ")))
    case None =>
      Error(1, "Setting not found: " + id)
  }
  
  private val dispatchTable = ServerConnection.locateSettings(getClass)
  
  
  private def findServer(server: Either[Long, String]): ServerHandler = server.fold(findServer, findServer)
    
  private def findServer(name: String): ServerHandler = hub.serverId(name) match {
    case Some(id) => findServer(id)
    case None => throw new Exception("Server not found: " + name)
  }
  
  private def findServer(id: Long): ServerHandler = hub.getHandler(id) match {
    case Some(server: ServerHandler) => server
    case None => throw new Exception("Server not found: " + id)
  }

  
  // general settings for lookup and help
  
  @Setting(id=1, name="Servers", doc="")
  @Return("*(ws)")
  def getServers: Data = {
    val servers = hub.handlers collect {
      case s: ServerHandler if s.isServing => s
    }
    Arr(servers sortBy (_.id) map { s => Cluster(Word(s.id), Str(s.name)) })
  }
  
  @Setting(id=2, name="Settings", doc="")
  @Return("*(ws)")
  def getSettings(data: Either[Long, String]): Data = {
    val server = findServer(data)
    val settings = server.settings.sorted
    Arr(settings map { case (id, name) => Cluster(Word(id), Str(name)) })
  }
    
  @Setting(id=3, name="Lookup", doc="")
  @Return("w")
  def lookup(server: String): Data = {
    val handler = findServer(server)
    Word(handler.id)
  }
  
  @Return("w, <w|*w>")
  def lookup(
      serverId: Either[Long, String],
      settingNames: Either[String, Seq[String]]): Data = {
    val server = findServer(serverId)
    def idFor(name: String) = Word(server.setting(name).get.id)
    val settingIds = settingNames match {
      case Left(name) => idFor(name)
      case Right(names) => Arr(names map idFor)
    }
    Cluster(Word(server.id), settingIds)
  }
  
  @Setting(id=10, name="Help", doc="")
  @Return("ss")
  def help(server: Either[Long, String]): Data = {
    val handler = findServer(server)
    Cluster(Str(handler.doc), Str(""))
  }
  
  @Return("s, *s, *s, s")
  def help(serverId: Either[Long, String], settingId: Either[Long, String]): Data = {
    val server = findServer(serverId)
    server.setting(settingId) match {
      case None =>
        settingId match {
          case Left(id) => Error(1, "Setting %d not found".format(id))
          case Right(name) => Error(1, "Setting '%s' not found".format(name))
        }
      case Some(setting) =>
        Cluster(
          Str(setting.doc),
          Arr(setting.accepted map { td => Str(td.tag) }),
          Arr(setting.returned map { td => Str(td.tag) }),
          Str("")
        )
    }
  }
  
  
  // contexts and messages
  
  @Setting(id=50, name="Expire Context", doc="")
  @Return("w")
  def expireContext(server: Either[Long, String]): Data = {
    Word(0)
  }
  
  @Setting(id=60, name="Subscribe to Named Message", doc="")
  def subscribeToNamedMessage(name: String, id: Long, active: Boolean) {
    if (active)
      messager.register(name, source.id, ctx, id)
    else
      messager.unregister(name, source.id, ctx, id)
  }
  
  @Setting(id=61, name="Send Named Message", doc="")
  def sendNamedMessage(name: String, message: Data) {
    messager.broadcast(name, message, source.id)
  }
  
  
  // server settings

  private def ensureServer: ServerHandler = source match {
    case server: ServerHandler => server
    case _ => throw new Exception("Setting only available for server connections")
  }
  
  @Setting(id=100, name="S: Register Setting", doc="")
  def registerSetting(
      id: Long,
      name: String,
      doc: String,
      accepts: Seq[String],
      returns: Seq[String],
      notes: String) {
    val server = ensureServer
    server.registerSetting(id, name, doc + "\n\n" + notes, accepts, returns)
  }
  
  @Setting(id=101, name="S: Unregister Setting", doc="")
  def unregisterSetting(setting: Either[Long, String]): Data = {
    val server = ensureServer
    setting match {
      case Left(id) => server.unregisterSetting(id)
      case Right(name) => server.unregisterSetting(name)
    }
    Data.EMPTY // TODO: what should be the return value here?
  }

  @Setting(id=110, name="S: Notify on Context Expiration", doc="")
  def notifyOnContextExpiration(data: Option[(Long, Boolean)]) {
    val server = ensureServer
    throw new Exception("not implemented")
    // TODO: implement context management
  }
  
  @Setting(id=120, name="S: Start Serving", doc="")
  def startServing() {
    val server = ensureServer
    server.startServing
  }

  
  // utility methods
  
  @Setting(id=200, name="Data to String", doc="")
  @Return("s")
  def dataToString(data: Data): Data = Str(data.toString) // TODO: implement this
  
  @Setting(id=201, name="String to Data", doc="")
  @Return("?")
  def stringToData(str: String): Data = Str("") // TODO: implement this
  
  // convert units
  // pretty print
  
  @Setting(id=13579, name="Echo", doc="Echoes back any data sent to this setting")
  @Return("?")
  def echo(data: Data): Data = data
}


trait Messager {
  def register(msg: String, target: Long, ctx: Context, id: Long): Unit
  def unregister(msg: String, target: Long, ctx: Context, id: Long): Unit
  def broadcast(msg: String, data: Data, sourceId: Long): Unit
  def disconnect(client: ClientHandler): Unit
}

class MessagerImpl(hub: HandlerHub) extends TypedActor with Messager {
  private var regs: Map[String, Set[(Long, Context, Long)]] = Map.empty
  
  def register(msg: String, target: Long, ctx: Context, id: Long) {
    regs += msg -> { regs.get(msg).getOrElse(Set()) + ((target, ctx, id)) }
  }

  def unregister(msg: String, target: Long, ctx: Context, id: Long) {
    regs += msg -> { regs.get(msg).getOrElse(Set()) - ((target, ctx, id)) }
  }

  def broadcast(msg: String, data: Data, sourceId: Long) {
    regs.get(msg) map { listeners =>
      for ((clientId, ctx, id) <- listeners)
        hub.getHandler(clientId) map { _.sendMessage(Packet(0, sourceId, ctx, Seq(Record(id, data)))) }
    }
  }

  def disconnect(client: ClientHandler) {
    regs = regs mapValues { _ filter { case (id, _, _) => id != client.id } }
  }
}


trait AuthService {
  def authenticate(challenge: Array[Byte], response: Array[Byte]): Boolean  
}

class AuthServiceImpl(password: String) extends AuthService {
  def authenticate(challenge: Array[Byte], response: Array[Byte]): Boolean = {
    val md = MessageDigest.getInstance("MD5")
    md.update(challenge)
    md.update(password.getBytes(Data.STRING_ENCODING))
    md.digest.toSeq == response.toSeq
  }
}

trait HandlerHub {
  def connectClient(channel: Channel, name: String): ClientHandler
  def connectServer(channel: Channel, name: String, doc: String): ClientHandler
  def handlers: Seq[ClientHandler]
  def getHandler(id: Long): Option[ClientHandler]
  def serverId(name: String): Option[Long]
  def disconnect(handler: ClientHandler): Unit
}

class HandlerHubImpl extends HandlerHub {
  private val handlerMap = mutable.Map.empty[Long, ClientHandler]
  private val idCache = mutable.Map.empty[String, Long]
  private val idPool = mutable.Buffer.empty[Long]
  private var _nextId = 1L
  private def nextId = if (!idPool.isEmpty) idPool.remove(0) else { val id = _nextId; _nextId += 1; id }
  
  def connectClient(channel: Channel, name: String) =
    connect(name, false) { (id, name) => new ClientHandlerImpl(this, channel, id, name) }
  
  def connectServer(channel: Channel, name: String, doc: String) =
    connect(name, true) { (id, name) => new ServerHandlerImpl(this, channel, id, name, doc) }
  
  def connect(name: String, isServer: Boolean)(factory: (Long, String) => ClientHandler) = synchronized {
    val id = if (isServer) idCache.get(name).getOrElse(nextId) else nextId
    handlerMap.get(id) match {
      case Some(handler) =>
        // already have a server logged in with this id
        throw new LabradException(6, "Server '%s' already exists".format(name))
      case None =>
        // id is currently unassigned
        val handler = factory(id, name)
        handlerMap(id) = handler
        if (isServer) idCache(name) = handler.id
        handler
    }
  }
  
  def disconnect(handler: ClientHandler) = synchronized {
    handler match {
      case server: ServerHandler => // don't release server id's
      case _ => idPool += handler.id
    }
    handlerMap -= handler.id
  }
  
  def getHandler(id: Long): Option[ClientHandler] = synchronized { handlerMap.get(id) }
  def serverId(name: String): Option[Long] = synchronized { idCache.get(name) }
  def handlers: Seq[ClientHandler] = synchronized { handlerMap.values.toSeq }
}


class Manager

object Manager {
  
  def main(args: Array[String]) {
    val factory = new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool,
        Executors.newCachedThreadPool)
    
    val executionHandler = new ExecutionHandler(
        new OrderedMemoryAwareThreadPoolExecutor(16, 1048576, 1048576))
    
    val channels = new DefaultChannelGroup("LabRAD-manager")
    
    val auth = new AuthServiceImpl("test")
    val hub = new HandlerHubImpl
    
    val bootstrap = new ServerBootstrap(factory)
    bootstrap.setOption("child.tcpNoDelay", true)
    bootstrap.setOption("child.keepAlive", true)
    bootstrap.setPipelineFactory(new ChannelPipelineFactory {
      val grouper = new ChannelGrouper(channels)
      val packetEncoder = new PacketEncoder
      val packetDecoder = new PacketDecoder
      
      def getPipeline = {
        val pipeline = Channels.pipeline
        pipeline.addLast("channelGrouper", grouper)
        pipeline.addLast("byteOrderDecoder", new ByteOrderDecoder)
        pipeline.addLast("packetEncoder", packetEncoder)
        pipeline.addLast("packetDecoder", packetDecoder)
        pipeline.addLast("executionHandler", executionHandler)
        pipeline.addLast("loginHandler", new LoginHandler(auth, hub))
        pipeline
      }
    })
        
    val messager = TypedActor.newInstance(classOf[Messager], new MessagerImpl(hub))
    val manager = TypedActor.newInstance(classOf[ManagerSettings], new ManagerSettingsImpl(hub, messager))
    
    hub.connect("Manager", true) { (id, name) => new ManagerHandler(id, name, manager) }
    
    val channel = bootstrap.bind(new InetSocketAddress(7682))
    channels.add(channel)
    
    awaitShutdownCommand
    
    channels.close.awaitUninterruptibly
    
    TypedActor.stop(manager)
    TypedActor.stop(messager)
    
    executionHandler.releaseExternalResources
    factory.releaseExternalResources
  }
  
  private def awaitShutdownCommand {
    val runLock = new Lock
    runLock.acquire
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run { runLock.release }
    })
    runLock.acquire
  }
}
