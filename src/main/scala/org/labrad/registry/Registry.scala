package org.labrad.registry

import org.labrad.{Reflect, RequestContext, Server, ServerInfo}
import org.labrad.annotations._
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
  def child(parent: Dir, name: String, create: Boolean): (Dir, Boolean)
  def dir(curDir: Dir): (Seq[String], Seq[String])
  def rmDir(dir: Dir, name: String): Unit

  def getValue(dir: Dir, key: String, default: Option[(Boolean, Data)]): Data
  def setValue(dir: Dir, key: String, value: Data): Unit
  def delete(dir: Dir, key: String): Unit
}

class Registry(id: Long, name: String, store: RegistryStore, hub: Hub, tracker: StatsTracker)
extends ServerActor with Logging {

  // enforce doing one thing at a time using an async semaphore
  private val semaphore = new AsyncSemaphore(1)

  private val contexts = mutable.Map.empty[Context, (RegistryContext, RequestContext => Data)]

  private val doc = "Provides a file system-like heirarchical storage of chunks of labrad data. Also allows clients to register for notifications when directories or keys are added or changed."
  private val (settings, bind) = Reflect.makeHandler[RegistryContext]

  // send info about this server and its settings to the manager
  hub.setServerInfo(ServerInfo(id, name, doc, settings))

  def expireContext(ctx: Context)(implicit timeout: Duration): Future[Long] = semaphore.map {
    expire(if (contexts.contains(ctx)) Seq(ctx) else Seq())
  }

  def expireAll(high: Long)(implicit timeout: Duration): Future[Long] = semaphore.map {
    expire(contexts.keys.filter(_.high == high).toSeq)
  }

  private def expire(ctxs: Seq[Context]): Long = ctxs match {
    case Seq() => 0L
    case _ =>
      log.debug(s"expiring contexts: ${ctxs.mkString(",")}")
      contexts --= ctxs
      1L
  }

  def close(): Unit = {}

  def message(packet: Packet): Unit = {
    tracker.msgRecv(id)
  }

  def request(packet: Packet)(implicit timeout: Duration): Future[Packet] = semaphore.map {
    // TODO: handle timeout
    tracker.serverReq(id)
    val response = Server.handle(packet, includeStackTrace = false) { req =>
      val (_, handler) = contexts.getOrElseUpdate(req.context, {
        val regCtx = new RegistryContext(req.context)
        val handler = bind(regCtx)
        (regCtx, handler)
      })
      handler(req)
    }
    tracker.serverRep(id)
    response
  }

  // send a notification to all contexts that have listeners registered
  private def messageContexts(dir: store.Dir, name: String, isDir: Boolean, addOrChange: Boolean) = {
    for ((ctx, _) <- contexts.values) {
      ctx.message(dir, name, isDir, addOrChange)
    }
  }

  // contains context-specific state and settings
  class RegistryContext(context: Context) {

    private var curDir = store.root
    private var changeListener: Option[(Long, Long)] = None // TODO: allow a context to send notifications to more than one target

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
      val (newDir, created) = store.child(dir, name, create)
      if (created) {
        messageContexts(dir, name, isDir=true, addOrChange=true)
      }
      newDir
    }

    @Setting(id=16,
             name="rmdir",
             doc="Delete the given subdirectory from the current directory")
    def rmDir(name: String): Unit = {
      store.rmDir(curDir, name)
      messageContexts(curDir, name, isDir=true, addOrChange=false)
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
      messageContexts(curDir, key, isDir=false, addOrChange=true)
    }

    @Setting(id=40,
             name="del",
             doc="Delete the given key from the current directory")
    def delete(key: String): Unit = {
      store.delete(curDir, key)
      messageContexts(curDir, key, isDir=false, addOrChange=false)
    }

    @Setting(id=50,
             name="Notify on Change",
             doc="Requests notifications if the contents of the current directory change")
    def notifyOnChange(r: RequestContext, id: Long, enable: Boolean): Unit = {
      changeListener = if (enable) Some((r.source, id)) else None
    }

    @Setting(id=100,
             name="Duplicate Context",
             doc="Copy context state from the specified context into the current context (DEPRECATED)")
    def duplicateContext(r: RequestContext, high: Long, low: Long): Unit = {
      val xHigh = if (high == 0) r.source else high
      contexts.get(Context(xHigh, low)) match {
        case None => sys.error(s"context ($xHigh, $low) does not exist")
        case Some((other, _)) =>
          curDir = other.curDir
          // XXX: Any other state to copy here?
      }
    }

    /**
     * Handle messages about added/changed/removed directories and keys
     *
     * If this context is currently in the same directory as the one for
     * the message, and has a message listener registered, then we will
     * send a message of the form (s{dirOrKeyName}, b{isDir}, b{addOrChange})
     */
    private[registry] def message(dir: store.Dir, name: String, isDir: Boolean, addOrChange: Boolean): Unit = {
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

