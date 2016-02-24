package org.labrad.registry

import org.labrad._
import org.labrad.annotations._
import org.labrad.concurrent.{Chan, Send, Time}
import org.labrad.concurrent.Go._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.manager.{Hub, ServerActor, StatsTracker}
import org.labrad.types._
import org.labrad.util.{AsyncSemaphore, Logging}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

trait RegistryStore {
  type Dir

  def root: Dir
  def pathTo(dir: Dir): Seq[String]
  def parent(dir: Dir): Dir
  def dir(curDir: Dir): (Seq[String], Seq[String])

  def child(parent: Dir, name: String, create: Boolean): Dir = {
    val (newDir, created) = childImpl(parent, name, create)
    if (created) {
      notifyListener(parent, name, isDir=true, addOrChange=true)
    }
    newDir
  }
  def childImpl(parent: Dir, name: String, create: Boolean): (Dir, Boolean)

  def rmDir(dir: Dir, name: String): Unit = {
    rmDirImpl(dir, name)
    notifyListener(dir, name, isDir=true, addOrChange=false)
  }
  def rmDirImpl(dir: Dir, name: String): Unit

  def getValue(dir: Dir, key: String, default: Option[(Boolean, Data)]): Data

  def setValue(dir: Dir, key: String, value: Data): Unit = {
    setValueImpl(dir, key, value)
    notifyListener(dir, key, isDir=false, addOrChange=true)
  }
  def setValueImpl(dir: Dir, key: String, value: Data): Unit

  def delete(dir: Dir, key: String): Unit = {
    deleteImpl(dir, key)
    notifyListener(dir, key, isDir=false, addOrChange=false)
  }
  def deleteImpl(dir: Dir, key: String): Unit

  @volatile private var listenerOpt: Option[(Dir, String, Boolean, Boolean) => Unit] = None

  def notifyOnChange(listener: (Dir, String, Boolean, Boolean) => Unit): Unit = {
    listenerOpt = Some(listener)
  }

  protected def notifyListener(dir: Dir, name: String, isDir: Boolean, addOrChange: Boolean): Unit = {
    for (listener <- listenerOpt) {
      listener(dir, name, isDir, addOrChange)
    }
  }
}

object Registry {
  val NAME = "Registry"
}

class RegistryActor(registry: Registry) extends ServerActor {
  val srcId = ""

  def message(packet: Packet): Unit = registry.message(srcId, packet)
  def request(packet: Packet)(implicit timeout: Duration): Future[Packet] = registry.request(srcId, packet)

  def expireContext(ctx: Context)(implicit timeout: Duration): Future[Long] = registry.expireContext(srcId, ctx)
  def expireAll(high: Long)(implicit timeout: Duration): Future[Long] = registry.expireAll(srcId, high)

  def close(): Unit = {}
}

class RegistryConnector(registry: Registry, config: ServerConfig) extends Logging {

  implicit val timeout = 30.seconds
  val reconnectDelay = 10.seconds
  val srcId = s"${config.host}:${config.port}"

  class RegistryServer(stopped: Send[Unit]) extends IServer {
    val name = registry.name
    val doc = registry.doc
    val settings = registry.settings

    def connected(cxn: Connection): Unit = {}
    def handleRequest(packet: Packet): Future[Packet] = registry.request(srcId, packet)
    def expire(context: Context): Unit = registry.expireContext(srcId, context)
    def stop(): Unit = {
      registry.expireAll(srcId)
      stopped.send(())
    }
  }

  @volatile private var connected = false
  def isConnected = connected

  sealed trait Msg
  case object Ping extends Msg
  case object Reconnect extends Msg
  case object Stop extends Msg

  private val ctrl = Chan[Msg](1)

  private val runFuture = go {
    var done = false
    while (!done) {
      log.info(s"$srcId: connecting...")
      val disconnected = Chan[Unit](1)
      val server = new RegistryServer(disconnected)

      val cxn: Connection = try {
        val cxn = Server.start(server, config)
        connected = true
        log.info(s"$srcId: connected")
        cxn
      } catch {
        case e: Exception =>
          log.error(s"$srcId: failed to connect", e)
          null
      }

      def doClose(): Unit = {
        try {
          cxn.close()
        } catch {
          case e: Exception =>
            log.error(s"$srcId: error during connection close", e)
        }
      }

      while (connected) {
        select(
          disconnected.onRecv { _ =>
            log.info(s"$srcId: connection lost; will reconnect after $reconnectDelay")
            connected = false
          },
          ctrl.onRecv {
            case Ping =>
              log.info(s"$srcId: ping")
              val mgr = new ManagerServerProxy(cxn)
              mgr.echo(Str("ping")).onFailure {
                case e: Exception =>
                  log.error(s"$srcId: error during ping", e)
              }

            case Reconnect =>
              doClose()
              log.info(s"$srcId: will reconnect after $reconnectDelay")
              connected = false

            case Stop =>
              doClose()
              log.info(s"$srcId: stopped")
              connected = false
              done = true
          }
        )
      }

      if (!done) {
        select(
          Time.after(reconnectDelay).onRecv { _ => },
          ctrl.onRecv {
            case Ping | Reconnect =>
            case Stop => done = true
          }
        )
      }
    }
  }

  def ping(): Unit = {
    ctrl.send(Ping)
  }

  def reconnect(): Unit = {
    ctrl.send(Reconnect)
  }

  def stop(): Future[Unit] = {
    ctrl.send(Stop)
    runFuture
  }
}


class Registry(id: Long, val name: String, store: RegistryStore, hub: Hub, tracker: StatsTracker, externalConfig: ServerConfig)
extends Logging {

  // enforce doing one thing at a time using an async semaphore
  private val semaphore = new AsyncSemaphore(1)

  type Src = String
  private val contexts = mutable.Map.empty[(Src, Context), (RegistryContext, RequestContext => Data)]
  private val managers = mutable.Map.empty[(String, Int), RegistryConnector]

  val doc = "Provides a file system-like heirarchical storage of chunks of labrad data. Also allows clients to register for notifications when directories or keys are added or changed."
  private val (_settings, bind) = Reflect.makeHandler[RegistryContext]

  val settings = _settings

  // send info about this server and its settings to the manager
  hub.setServerInfo(ServerInfo(id, name, doc, settings))

  def expireContext(src: Src, ctx: Context)(implicit timeout: Duration): Future[Long] = semaphore.map {
    expire(if (contexts.contains((src, ctx))) Seq((src, ctx)) else Seq())
  }

  def expireAll(src: Src, high: Long)(implicit timeout: Duration): Future[Long] = semaphore.map {
    expire(contexts.keys.filter { case (s, ctx) => s == src && ctx.high == high }.toSeq)
  }

  def expireAll(src: Src)(implicit timeout: Duration): Future[Unit] = semaphore.map {
    expire(contexts.keys.filter { case (s, _) => s == src }.toSeq)
  }

  private def expire(ctxs: Seq[(Src, Context)]): Long = ctxs match {
    case Seq() => 0L
    case _ =>
      log.debug(s"expiring contexts: ${ctxs.mkString(",")}")
      contexts --= ctxs
      1L
  }

  def message(src: Src, packet: Packet): Unit = {
    tracker.msgRecv(id)
  }

  def request(src: Src, packet: Packet)(implicit timeout: Duration): Future[Packet] = semaphore.map {
    // TODO: handle timeout
    tracker.serverReq(id)
    val response = Server.handle(packet, includeStackTrace = false) { req =>
      val (_, handler) = contexts.getOrElseUpdate((src, req.context), {
        val regCtx = new RegistryContext(src, req.context)
        val handler = bind(regCtx)
        (regCtx, handler)
      })
      handler(req)
    }
    tracker.serverRep(id)
    response
  }

  // send a notification to all contexts that have listeners registered
  private def notifyContexts(dir: store.Dir, name: String, isDir: Boolean, addOrChange: Boolean): Unit = {
    semaphore.map {
      for ((ctx, _) <- contexts.values) {
        ctx.notify(dir, name, isDir, addOrChange)
      }
    }
  }

  // tell the backend to call us when changes are made
  store.notifyOnChange(notifyContexts)

  private def refreshManagers(): Unit = {
    // load the list of managers from the registry
    var dir = store.root
    dir = store.child(dir, "Servers", create = true)
    dir = store.child(dir, "Registry", create = true)
    dir = store.child(dir, "Multihead", create = true)
    val default = DataBuilder("*(sws)").array(0).result()
    val result = store.getValue(dir, "Managers", default = Some((true, default)))
    val configs = result.get[Seq[(String, Long, String)]].map {
      case (host, port, pw) =>
        externalConfig.copy(
          host = host,
          port = if (port != 0) port.toInt else externalConfig.port,
          password = if (pw != "") pw.toCharArray else externalConfig.password
        )
    }
    val urls = configs.map(c => s"${c.host}:${c.port}")
    log.info(s"managers in registry: ${urls.mkString(", ")}")

    // connect to any managers we are not already connected to
    for (config <- configs) {
      managers.getOrElseUpdate((config.host, config.port), {
        new RegistryConnector(Registry.this, config)
      })
    }
  }

  // connect to managers that are stored in the registry
  refreshManagers()

  // contains context-specific state and settings
  class RegistryContext(src: Src, context: Context) {

    private var curDir = store.root
    private var changeListener: Option[(Long, Long)] = None
    private var allChangeListener: Option[(Long, Long)] = None

    @Setting(id=1,
             name="dir",
             doc="Returns lists of the subdirs and keys in the current directory")
    def listDir(): (Seq[String], Seq[String]) = {
      store.dir(curDir)
    }

    private def dirForPath(dirs: Seq[String], create: Boolean): store.Dir = {
      var newDir = curDir
      for ((dir, i) <- dirs.zipWithIndex) dir match {
        case ""   => if (i == 0) newDir = store.root
        case "."  =>
        case ".." => newDir = store.parent(newDir)
        case dir  => newDir = _mkDir(newDir, dir, create = create)
      }
      newDir
    }

    @Setting(id=10,
             name="cd",
             doc="Change the current directory")
    def changeDir(): Seq[String] = {
      store.pathTo(curDir)
    }
    def changeDir(dir: Either[String, Seq[String]], create: Boolean = false): Seq[String] = {
      val dirs = dir match {
        case Left(dir) => Seq(dir)
        case Right(dirs) => dirs
      }

      curDir = dirForPath(dirs, create = create)
      store.pathTo(curDir)
    }

    // FIXME: accepting numbers is a kludge to allow the delphi registry editor to work
    def changeDir(dir: Long): Seq[String] = {
      changeDir(Right(Seq.fill(dir.toInt)("..")))
    }

    @Setting(id=15,
             name="mkdir",
             doc="Create a new subdirectory in the current directory with the given name")
    def mkDir(name: String): Seq[String] = {
      val dir = _mkDir(curDir, name, create = true)
      store.pathTo(dir)
    }

    /**
     * Make a directory and send a message to all interested listeners.
     */
    private def _mkDir(dir: store.Dir, name: String, create: Boolean): store.Dir = {
      if (create) require(name.nonEmpty, "Cannot create a directory with an empty name")
      store.child(dir, name, create)
    }

    @Setting(id=16,
             name="rmdir",
             doc="Delete the given subdirectory from the current directory")
    def rmDir(name: String): Unit = {
      store.rmDir(curDir, name)
    }

    @Setting(id=20,
             name="get",
             doc="Get the content of the given key in the current directory")
    def getValue(key: String): Data = _getValue(key)
    def getValue(key: String, pat: String): Data = _getValue(key, pat)
    def getValue(key: String, set: Boolean, default: Data): Data = _getValue(key, default = Some((set, default)))
    def getValue(key: String, pat: String, set: Boolean, default: Data): Data = _getValue(key, pat, Some((set, default)))

    def _getValue(key: String, pat: String = "?", default: Option[(Boolean, Data)] = None): Data = {
      val data = store.getValue(curDir, key, default)
      val pattern = Pattern(pat)
      data.convertTo(pattern)
    }

    @Setting(id=30,
             name="set",
             doc="Set the content of the given key in the current directory to the given data")
    def setValue(key: String, value: Data): Unit = {
      require(key.nonEmpty, "Cannot create a key with empty name")
      store.setValue(curDir, key, value)
    }

    @Setting(id=40,
             name="del",
             doc="Delete the given key from the current directory")
    def delete(key: String): Unit = {
      store.delete(curDir, key)
    }

    @Setting(id=50,
             name="Notify on Change",
             doc="Requests notifications if the contents of the current directory change")
    def notifyOnChange(r: RequestContext, id: Long, enable: Boolean): Unit = {
      changeListener = if (enable) Some((r.source, id)) else None
    }

    @Setting(id=55,
             name="Stream Changes",
             doc="Requests notifications of all changes made in any directory")
    def streamChanges(r: RequestContext, id: Long, enable: Boolean): Unit = {
      allChangeListener = if (enable) Some((r.source, id)) else None
    }

    @Setting(id=100,
             name="Duplicate Context",
             doc="Copy context state from the specified context into the current context (DEPRECATED)")
    def duplicateContext(r: RequestContext, high: Long, low: Long): Unit = {
      val xHigh = if (high == 0) r.source else high
      contexts.get((src, Context(xHigh, low))) match {
        case None => sys.error(s"context ($xHigh, $low) does not exist")
        case Some((other, _)) =>
          curDir = other.curDir
          // XXX: Any other state to copy here?
      }
    }

    @Setting(id=1000,
             name="Managers",
             doc="Get a list of managers we are connecting to as an external registry")
    def managersList(): Seq[(String, Int, Boolean)] = {
      managers.toSeq.map { case ((host, port), reg) =>
        (host, port, reg.isConnected)
      }.sorted
    }

    @Setting(id=1001,
             name="Managers Refresh",
             doc="Refresh the list of managers from the registry.")
    def managersRefresh(): Unit = {
      refreshManagers()
    }

    @Setting(id=1002,
             name="Managers Add",
             doc="Add a new manager to connect to as an external registry")
    def managersAdd(host: String, port: Option[Int], password: Option[String]): Unit = {
      val config = externalConfig.copy(
        host = host,
        port = port.getOrElse(externalConfig.port),
        password = password.map(_.toCharArray).getOrElse(externalConfig.password)
      )
      managers.getOrElseUpdate((config.host, config.port), {
        new RegistryConnector(Registry.this, config)
      })
    }

    @Setting(id=1003,
             name="Managers Ping",
             doc="Send a network ping to all external managers")
    def managersPing(hostPat: String = ".*", port: Int = 0): Unit = {
      for ((_, reg) <- matchingManagers(hostPat, port)) {
        reg.ping()
      }
    }

    @Setting(id=1004,
             name="Managers Reconnect",
             doc="Disconnect from matching managers and reconnect")
    def managersReconnect(hostPat: String, port: Int = 0): Unit = {
      for ((key, reg) <- matchingManagers(hostPat, port.toInt)) {
        reg.reconnect()
        managers.remove(key)
      }
    }

    @Setting(id=1005,
             name="Managers Drop",
             doc="Disconnect from matching managers and do not reconnect")
    def managersDrop(hostPat: String, port: Int = 0): Unit = {
      for ((key, reg) <- matchingManagers(hostPat, port.toInt)) {
        reg.stop()
        managers.remove(key)
      }
    }

    private def matchingManagers(hostPat: String, port: Int): Seq[((String, Int), RegistryConnector)] = {
      val hostRegex = hostPat.r
      for {
        ((h, p), reg) <- managers.toSeq
        if hostRegex.unapplySeq(h).isDefined
        if port == 0 || port == p
      } yield {
        ((h, p), reg)
      }
    }

    /**
     * Handle messages about added/changed/removed directories and keys
     *
     * If this context is currently in the same directory as the one for
     * the message, and has a message listener registered, then we will
     * send a message of the form (s{dirOrKeyName}, b{isDir}, b{addOrChange})
     */
    private[registry] def notify(dir: store.Dir, name: String, isDir: Boolean, addOrChange: Boolean): Unit = {
      for ((target, msgId) <- allChangeListener) {
        log.debug(s"notify: ctx=${context} name=${name} isDir=${isDir} addOrChange=${addOrChange}")
        val msg = Cluster(Arr(store.pathTo(dir).toArray), Str(name), Bool(isDir), Bool(addOrChange))
        val pkt = Packet(0, target = id, context = context, records = Seq(Record(msgId, msg)))
        hub.message(target, pkt)
      }
      if (dir == curDir) {
        for ((target, msgId) <- changeListener) {
          log.debug(s"notify: ctx=${context} name=${name} isDir=${isDir} addOrChange=${addOrChange}")
          val msg = Cluster(Str(name), Bool(isDir), Bool(addOrChange))
          val pkt = Packet(0, target = id, context = context, records = Seq(Record(msgId, msg)))
          hub.message(target, pkt)
        }
      }
    }
  }
}
