package org.labrad.registry

import org.labrad._
import org.labrad.annotations._
import org.labrad.concurrent.{Chan, Send, Time}
import org.labrad.concurrent.Go._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.manager.{LocalServer, MultiheadServer, RemoteConnector, ServerActor}
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

class Registry(val id: Long, val name: String, store: RegistryStore, externalConfig: ServerConfig)
extends LocalServer with Logging {

  // enforce doing one thing at a time using an async semaphore
  private val semaphore = new AsyncSemaphore(1)

  type Src = String
  private val contexts = mutable.Map.empty[(Src, Context), (RegistryContext, RequestContext => Data)]

  val doc = "Provides a file system-like heirarchical storage of chunks of labrad data. Also allows clients to register for notifications when directories or keys are added or changed."
  private val (_settings, bind) = Reflect.makeHandler[RegistryContext]

  val settings = _settings

  // start multi-headed connections to other managers
  private val multihead = new MultiheadServer(name, store, this, externalConfig)

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

  def message(src: Src, packet: Packet): Unit = {}

  def request(src: Src, packet: Packet, messageFunc: (Long, Packet) => Unit)(implicit timeout: Duration): Future[Packet] = semaphore.map {
    // TODO: handle timeout
    val response = Server.handle(packet, includeStackTrace = false) { req =>
      val (_, handler) = contexts.getOrElseUpdate((src, req.context), {
        val regCtx = new RegistryContext(src, req.context, messageFunc)
        val handler = bind(regCtx)
        (regCtx, handler)
      })
      handler(req)
    }
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

  // contains context-specific state and settings
  class RegistryContext(src: Src, context: Context, messageFunc: (Long, Packet) => Unit) {

    private var curDir = store.root
    private var changeListener: Option[(Long, Long)] = None
    private var allChangeListener: Option[(Long, Long)] = None

    @Setting(id = 1,
             name = "dir",
             doc = """Returns lists of the subdirs and keys in the current directory.""")
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

    @Setting(id = 10,
             name = "cd",
             doc = """Change the current directory.
                 |
                 |Called with a string giving a subdirectory of the current directory or ".." to
                 |change to the parent directory, or a sequence of strings giving a path to change
                 |into. The empty string refers to the root directory, so paths beginning with ""
                 |are absolute, while other paths are relative to the current directory.
                 |
                 |Optionally takes a second boolean argument whether to create the directory or path
                 |if it does not exist.
                 |
                 |Can also be called with a single integer argument n to change to the nth parent of
                 |of the current directory, that is to go n levels back toward the root directory.
                 |This usage is deprecated.""")
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

    @Setting(id = 15,
             name = "mkdir",
             doc = """Create a new subdirectory in the current directory with the given name.""")
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

    @Setting(id = 16,
             name = "rmdir",
             doc = """Delete the given subdirectory from the current directory.""")
    def rmDir(name: String): Unit = {
      store.rmDir(curDir, name)
    }

    @Setting(id = 20,
             name = "get",
             doc = """Get the content of the given key in the current directory.
                 |
                 |Can be called in one of four ways, with one of the following:
                 |- Key name. Returns an error if not found.
                 |- Key name, type tag. The value will be converted to the given type before being
                 |  returned. Returns an error if key not found or cannot be converted to the
                 |  requested type.
                 |- Key name, set flag, default value. Retrieves the given key, or if not found
                 |  returns the given default value. Also, if the set flag is true it will be set to
                 |  the default value in the registry before being returned.
                 |- Key name, type tag, set flag, default value. Combines the behavior of the
                 |  previous two, by getting the given default value if the key is not found and
                 |  optionally setting it in the registry, then converting the value to the given
                 |  type before returning it.""")
    def getValue(key: String): Data = _getValue(key)
    def getValue(key: String, pat: String): Data = _getValue(key, pat)
    def getValue(key: String, set: Boolean, default: Data): Data = _getValue(key, default = Some((set, default)))
    def getValue(key: String, pat: String, set: Boolean, default: Data): Data = _getValue(key, pat, Some((set, default)))

    def _getValue(key: String, pat: String = "?", default: Option[(Boolean, Data)] = None): Data = {
      val data = store.getValue(curDir, key, default)
      val pattern = Pattern(pat)
      data.convertTo(pattern)
    }

    @Setting(id = 30,
             name = "set",
             doc = """Set the given key in the current directory to the given value.""")
    def setValue(key: String, value: Data): Unit = {
      require(key.nonEmpty, "Cannot create a key with empty name")
      store.setValue(curDir, key, value)
    }

    @Setting(id = 40,
             name = "del",
             doc = """Delete the given key from the current directory.""")
    def delete(key: String): Unit = {
      store.delete(curDir, key)
    }

    @Setting(id = 50,
             name = "Notify on Change",
             doc = """Requests notifications if the contents of the current directory change.
                 |
                 |Called with message id and a flag indicating whether notifications should be
                 |enabled or disabled. Notification messages will be sent to the given id in the
                 |context used when calling this setting. The message will be of the form
                 |(name, isDir, addOrChange), where name is the name of an item in the current
                 |directory, isDir is a boolean indicating whether the item is a directory (true) or
                 |key (false), and addOrChange is a boolean indicating whether the item was added or
                 |changed (true), or was deleted (false).""")
    def notifyOnChange(r: RequestContext, id: Long, enable: Boolean): Unit = {
      changeListener = if (enable) Some((r.source, id)) else None
    }

    @Setting(id = 55,
             name = "Stream Changes",
             doc = """Requests notifications of all changes made in any directory.
                 |
                 |Called with message id and a flag indicating whether notifications should be
                 |enabled or disabled. Notification messages will be sent to the given id in the
                 |context used when calling this setting. The message will be of the form
                 |(path, name, isDir, addOrChange), where path is an absolute registry path given as
                 |a list of strings, name is the name of an item in the specified path, isDir is a
                 |boolean indicating whether the item is a directory (true) or key (false), and
                 |addOrChange is a boolean indicating whether the item was added or changed (true),
                 |or was deleted (false).
                 |
                 |Note that notifications are sent for changes occuring in any registry directory,
                 |regardless of the current directory when this setting is called.""")
    def streamChanges(r: RequestContext, id: Long, enable: Boolean): Unit = {
      allChangeListener = if (enable) Some((r.source, id)) else None
    }

    @Setting(id = 100,
             name = "Duplicate Context",
             doc = """Copy context state from the specified context into the request context.
                 |
                 |DEPRECATED""")
    def duplicateContext(r: RequestContext, high: Long, low: Long): Unit = {
      val xHigh = if (high == 0) r.source else high
      contexts.get((src, Context(xHigh, low))) match {
        case None => sys.error(s"context ($xHigh, $low) does not exist")
        case Some((other, _)) =>
          curDir = other.curDir
          // XXX: Any other state to copy here?
      }
    }

    @Setting(id = 1000,
             name = "Managers",
             doc = """Get a list of managers we are connecting to as an external registry.
                 |
                 |The returned list is a sequence of clusters of the form (host, port, connected),
                 |where host is the string hostname and port the integer port number of the manager,
                 |and connected is a flag indicating whether we are currently connected to that
                 |manager.""")
    def managersList(): Seq[(String, Int, Boolean)] = {
      multihead.list()
    }

    @Setting(id = 1001,
             name = "Managers Refresh",
             doc = """Refresh the list of managers from the registry.""")
    def managersRefresh(): Unit = {
      multihead.refresh()
    }

    @Setting(id = 1002,
             name = "Managers Add",
             doc = """Add a new manager to connect to as an external registry.
                 |
                 |Specified as a hostname, optional port number, and optional password. If port is
                 |not given, use the default labrad port. If password is not given, use the same
                 |password as configured on the manager where the registry is running locally.""")
    def managersAdd(host: String, port: Option[Int], password: Option[String]): Unit = {
      multihead.add(host, port, password)
    }

    @Setting(id = 1003,
             name = "Managers Ping",
             doc = """Send a network ping to matching external managers.
                 |
                 |Called with a string giving an optional regular expression to match against
                 |manager names, and an optional port number. If no port is given, matches any port.
                 |If no host regex is given, matches any hostname.""")
    def managersPing(hostPat: String = ".*", port: Int = 0): Unit = {
      multihead.ping(hostPat, port)
    }

    @Setting(id = 1004,
             name = "Managers Reconnect",
             doc = """Disconnect from matching managers and reconnect.
                 |
                 |Hostname regular expression and port number are matched against external managers
                 |as described for "Managers Ping".""")
    def managersReconnect(hostPat: String, port: Int = 0): Unit = {
      multihead.reconnect(hostPat, port)
    }

    @Setting(id = 1005,
             name = "Managers Drop",
             doc = """Disconnect from matching managers and do not reconnect.
                 |
                 |Hostname regular expression and port number are matched against external managers
                 |as described for "Managers Ping".""")
    def managersDrop(hostPat: String, port: Int = 0): Unit = {
      multihead.drop(hostPat, port)
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
        messageFunc(target, pkt)
      }
      if (dir == curDir) {
        for ((target, msgId) <- changeListener) {
          log.debug(s"notify: ctx=${context} name=${name} isDir=${isDir} addOrChange=${addOrChange}")
          val msg = Cluster(Str(name), Bool(isDir), Bool(addOrChange))
          val pkt = Packet(0, target = id, context = context, records = Seq(Record(msgId, msg)))
          messageFunc(target, pkt)
        }
      }
    }
  }
}
