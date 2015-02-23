package org.labrad.registry

import java.io.{ByteArrayOutputStream, File, FileInputStream, FileOutputStream}
import java.net.{URLDecoder, URLEncoder}
import java.nio.ByteOrder.BIG_ENDIAN
import java.nio.charset.StandardCharsets.UTF_8
import org.labrad.{Reflect, RequestContext, Server, ServerInfo}
import org.labrad.annotations._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.manager.{Hub, ServerActor, StatsTracker}
import org.labrad.types._
import org.labrad.util.{AsyncSemaphore, Logging}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

object Registry {
  val EXT = ".txt"
}

class Registry(id: Long, name: String, rootDir: File, hub: Hub, tracker: StatsTracker)
extends ServerActor with Logging {

  // enforce doing one thing at a time using an async semaphore
  private val semaphore = new AsyncSemaphore(1)

  // create the root directory if it does not already exist
  if (!rootDir.exists) rootDir.mkdir

  private var contexts = mutable.Map.empty[Context, (RegistryContext, RequestContext => Data)]

  private val doc = "Provides a file system-like heirarchical storage of chunks of labrad data. Also allows clients to register for notifications when directories or keys are added or changed."
  private val (settings, bind) = Reflect.makeHandler[RegistryContext]

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
    val response = Server.handle(packet) { case req @ RequestContext(source, ctx, id, data) =>
      val (_, handler) = contexts.getOrElseUpdate(ctx, {
        val regCtx = new RegistryContext(ctx, rootDir, this)
        val handler = bind(regCtx)
        (regCtx, handler)
      })
      handler(req)
    }
    tracker.serverRep(id)
    response
  }

  // used by contexts
  private[registry] def foreachContext(func: RegistryContext => Unit) = contexts.values.foreach { case (ctx, _) => func(ctx) }
  private[registry] def sendMessage(target: Long, packet: Packet): Unit = hub.message(target, packet.copy(target = id))
  private[registry] def getContext(context: Context): Option[RegistryContext] = contexts.get(context).map(_._1)
}

class RegistryContext(context: Context, rootDir: File, parent: Registry) extends Logging {
  import Registry._

  implicit val byteOrder = BIG_ENDIAN

  var curDir: File = rootDir

  @Setting(id=1,
           name="dir",
           doc="Returns lists of the subdirs and keys in the current directory")
  def listDir(): (Seq[String], Seq[String]) = {
    val files = curDir.listFiles
    val dirs = for (f <- files if f.isDirectory; n = f.getName) yield decode(n)
    val keys = for (f <- files if f.isFile; n = f.getName if n.endsWith(EXT)) yield decode(n.dropRight(EXT.size))
    (dirs, keys)
  }

  @Setting(id=10,
           name="cd",
           doc="Change the current directory")
  def changeDir(): Seq[String] = changeDir(Left(""), false)
  // FIXME: accepting numbers here is a kludge to allow the current registry editor to work
  //def changeDir(r: RequestContext, dir: Either[String, Seq[String]]): Data = changeDir(r, dir, false)
  def changeDir(dir: Either[Either[String, Seq[String]], Long]): Seq[String] = {
    val theDir = dir match {
      case Left(dir) => dir
      case Right(num) => Left(Array.fill(num.toInt)("..").mkString("/"))
    }
    changeDir(theDir, false)
  }
  def changeDir(dir: Either[String, Seq[String]], create: Boolean): Seq[String] = {
    def split(s: String): Seq[String] = s match {
      case "" => Seq()
      case "/" => Seq("")
      case s => s.split("/").toSeq
    }
    val dirs = dir match {
      case Left(dir) => split(dir)
      case Right(dirs) => dirs
    }

    var path = curDir
    for ((dir, i) <- dirs.zipWithIndex) dir match {
      case ""   => if (i == 0) path = rootDir
      case "."  =>
      case ".." => if (path != rootDir) path = path.getParentFile
      case dir  => path = new File(path, encode(dir))
    }
    if (!path.exists) {
      if (create)
        path.mkdirs()
      else
        sys.error(s"directory does not exist: ${regPathStr(path)}")
    }
    curDir = path
    regPath(curDir)
  }

  @Setting(id=15,
           name="mkdir",
           doc="Create a new subdirectory in the current directory with the given name")
  def mkDir(name: String): Seq[String] = {
    val path = new File(curDir, encode(name))
    if (!path.exists) path.mkdir()
    parent.foreachContext(_.notify(curDir, name, isDir=true, addOrChange=true))
    regPath(path)
  }

  @Setting(id=16,
           name="rmdir",
           doc="Delete the given subdirectory from the current directory")
  def rmDir(name: String): Unit = {
    val path = new File(curDir, encode(name))
    if (!path.exists) sys.error(s"directory does not exist: $name")
    if (!path.isDirectory) sys.error(s"found file instead of directory: $name")
    path.delete()
    parent.foreachContext(_.notify(curDir, name, isDir=true, addOrChange=false))
  }

  @Setting(id=20,
           name="get",
           doc="Get the content of the given key in the current directory")
  def getValue(key: String): Data = getValue(key, false, Data.NONE)
  def getValue(key: String, pat: String): Data = getValue(key, pat, false, Data.NONE)
  def getValue(key: String, set: Boolean, default: Data): Data = getValue(key, "?", set, default)
  def getValue(key: String, pat: String, set: Boolean, default: Data): Data = {
    val path = keyFile(key)
    if (!path.exists) {
      if (set)
        setValue(key, default)
      else
        sys.error(s"key does not exist: $key")
    }
    val Cluster(Str(typ), Bytes(bytes)) = Data.fromBytes(readFile(path), Type("ss"))
    val data = Data.fromBytes(bytes, Type(typ))
    val pattern = Pattern(pat)
    data.convertTo(pattern)
  }

//  @Setting(id=21,
//           name="getAsString",
//           doc="Get the content of the given key in the current directory")
//  def getValueStr(key: String): String = getValueStr(key, false, "")
//  def getValueStr(key: String, pat: String): String = getValueStr(key, pat, false, "")
//  def getValueStr(key: String, set: Boolean, default: String): String = getValueStr(key, "?", set, default)
//  def getValueStr(key: String, pat: String, set: Boolean, default: String): String = {
//    val path = keyFile(key)
//    if (!path.exists) {
//      if (set)
//        setValueStr(key, default)
//      else
//        sys.error("key does not exist: " + key)
//    }
//    val text = readFile(path)
//    val data = Data.parse(text)
//    val clone = data.clone
//    val pattern = Pattern(pat)
//    clone.convertTo(pattern)
//    if (clone == data) text else clone.toString
//  }

  @Setting(id=30,
           name="set",
           doc="Set the content of the given key in the current directory to the given data")
  def setValue(key: String, value: Data): Unit = {
    val path = keyFile(key)
    val data = Cluster(Str(value.t.toString), Bytes(value.toBytes))
    writeFile(path, data.toBytes)
    parent.foreachContext(_.notify(curDir, key, isDir=false, addOrChange=true))
  }

//  @Setting(id=31,
//           name="setAsString",
//           doc="Set the content of the given key in the current directory to the given data")
//  def setValueStr(r: RequestContext, key: String, text: String): Unit = {
//    val path = keyFile(key)
//    val data = Data.parse(text) // make sure text parses
//    writeFile(path, text)
//    parent.foreachContext(_.notify(curDir, key, isDir=false, addOrChange=true))
//  }

  @Setting(id=40,
           name="del",
           doc="Delete the given key from the current directory")
  def delete(key: String): Unit = {
    val path = keyFile(key)
    if (!path.exists) sys.error(s"key does not exist: $key")
    if (path.isDirectory) sys.error(s"found directory instead of file: $key")
    path.delete
    parent.foreachContext(_.notify(curDir, key, isDir=false, addOrChange=false))
  }

  @Setting(id=50,
           name="Notify On Change",
           doc="Requests notifications if the contents of the current directory change")
  def notifyOnChange(r: RequestContext, id: Long, enable: Boolean): Unit = {
    changeListener = if (enable) Some((r.source, id)) else None
  }

  // TODO: allow a context to send notifications to more than one target
  var changeListener: Option[(Long, Long)] = None

  // message: (name, isDir, addOrChange)
  def notify(dir: File, name: String, isDir: Boolean, addOrChange: Boolean): Unit = {
    log.debug(s"notify: ctx=${context} name=${name} isDir=${isDir} addOrChange=${addOrChange}")
    if (dir == curDir) {
      for ((target, id) <- changeListener) {
        val msg = Cluster(Str(name), Bool(isDir), Bool(addOrChange))
        parent.sendMessage(target, Packet(0, 0, context, Seq(Record(id, msg))))
      }
    }
  }


  @Setting(id=100,
           name="Duplicate Context",
           doc="Copy context state from the specified context into the current context")
  def duplicateContext(r: RequestContext, high: Long, low: Long): Unit = {
    val xHigh = if (high == 0) r.source else high
    parent.getContext(Context(xHigh, low)) match {
      case None => sys.error(s"context ($xHigh, $low) does not exist")
      case Some(other) =>
        curDir = other.curDir
        // XXX: Any other state to copy here?
    }
  }


  private def regPath(path: File): Seq[String] = {
    @tailrec
    def fun(path: File, rest: Seq[String]): Seq[String] =
      if (path == rootDir)
        "" +: rest
      else
        fun(path.getParentFile, decode(path.getName) +: rest)
    fun(path, Nil)
  }

  private def regPathStr(path: File): String = regPath(path).mkString("/")

  private def readFile(path: File): Array[Byte] = {
    val is = new FileInputStream(path)
    try {
      val os = new ByteArrayOutputStream()
      val buf = new Array[Byte](10000)
      var done = false
      while (!done) {
        val read = is.read(buf)
        if (read >= 0) os.write(buf, 0, read)
        done = read < 0
      }
      os.toByteArray
    } finally {
      is.close
    }
  }

  private def writeFile(path: File, contents: Array[Byte]) {
    val os = new FileOutputStream(path)
    try
      os.write(contents)
    finally
      os.close
  }

  private def keyFile(key: String) = new File(curDir, encode(key) + EXT)

  private def encode(segment: String): String = {
    URLEncoder.encode(segment, UTF_8.name)
  }

  private def decode(segment: String): String = {
    URLDecoder.decode(segment, UTF_8.name)
  }
}
