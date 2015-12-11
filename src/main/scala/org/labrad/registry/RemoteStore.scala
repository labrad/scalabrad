package org.labrad.registry

import org.labrad.{Client, RegistryServerPacket, RegistryServerProxy, TlsMode}
import org.labrad.data.{Data, Message}
import org.labrad.types.Type
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object RemoteStore {
  def apply(host: String, port: Int, password: Array[Char], tls: TlsMode): RemoteStore = {
    new RemoteStore(host, port, password, tls)
  }
}

/**
 * A registry store that proxies requests to another remote store.
 * For simplicity, we do not proxy messages, so clients connected
 * to this backend will not be notified of changes made by other
 * clients that connect to the same remote registry.
 */
class RemoteStore(host: String, port: Int, password: Array[Char], tls: TlsMode) extends RegistryStore {

  type Dir = Seq[String]

  val root = Seq("")

  private val lock = new Object
  private var reg: RegistryServerProxy = null
  private var listenerOpt: Option[(Dir, String, Boolean, Boolean) => Unit] = None

  override def notifyOnChange(listener: (Dir, String, Boolean, Boolean) => Unit): Unit = {
    listenerOpt = Some(listener)
  }

  /**
   * Connect to remote registry if we are not currently connected,
   * and listener for disconnection events so we can reconnect later.
   */
  private def connect(): Unit = {
    lock.synchronized {
      if (reg == null) {
        val cxn = new Client(name = "Registry Proxy", host = host, port = port, password = password, tls=tls)
        val id = 123456L
        cxn.addConnectionListener { case false =>
          lock.synchronized {
            reg = null
          }
        }
        cxn.addMessageListener {
          case Message(_, _, `id`, data) =>
            val (path, name, isDir, addOrChange) = data.get[(Seq[String], String, Boolean, Boolean)]
            for (listener <- listenerOpt) {
              listener(path, name, isDir, addOrChange)
            }
        }
        cxn.connect()
        val registry = new RegistryServerProxy(cxn)
        Await.result(registry.streamChanges(id, true), 5.seconds)
        reg = registry
      }
    }
  }

  // make sure we can connect when the manager first starts up
  connect()

  private def call[A](dir: Dir)(f: RegistryServerPacket => Future[A]): A = {
    connect()
    val p = reg.packet()
    p.cd(dir)
    val result = f(p)
    p.cd(root)
    p.send()
    Await.result(result, 10.seconds) // TODO: does this need to vary?
  }

  def pathTo(dir: Dir): Seq[String] = dir

  def parent(dir: Dir): Dir = if (dir == root) dir else dir.dropRight(1)

  def child(dir: Dir, name: String, create: Boolean): (Dir, Boolean) = {
    val (dirs, keys) = call(dir) { _.dir() }

    val created = if (dirs.contains(name)) {
      false
    } else {
      if (!create) sys.error(s"directory does not exist: $name")
      call(dir) { _.mkDir(name) }
      true
    }
    (dir :+ name, created)
  }

  def dir(curDir: Dir): (Seq[String], Seq[String]) = {
    call(curDir) { _.dir() }
  }

  def rmDir(dir: Dir, name: String): Unit = {
    call(dir) { _.rmDir(name) }
  }

  def getValue(dir: Dir, key: String, default: Option[(Boolean, Data)]): Data = {
    call(dir) { _.get(key, default = default) }
  }

  def setValue(dir: Dir, key: String, value: Data): Unit = {
    call(dir) { _.set(key, value) }
  }

  def delete(dir: Dir, key: String): Unit = {
    call(dir) { _.del(key) }
  }
}
