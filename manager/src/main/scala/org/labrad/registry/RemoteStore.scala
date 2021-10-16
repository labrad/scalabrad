package org.labrad.registry

import org.labrad.{Client, Credential, ManagerServerProxy, RegistryServerPacket,
    RegistryServerProxy, TlsMode}
import org.labrad.data.{Data, Message}
import org.labrad.types.Type
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object RemoteStore {
  def apply(host: String, port: Int, credential: Credential, tls: TlsMode): RemoteStore = {
    new RemoteStore(host, port, credential, tls)
  }
}

/**
 * A registry store that proxies requests to another remote store.
 * For simplicity, we do not proxy messages, so clients connected
 * to this backend will not be notified of changes made by other
 * clients that connect to the same remote registry.
 */
class RemoteStore(host: String, port: Int, credential: Credential, tls: TlsMode)
extends RegistryStore {

  type Dir = Seq[String]

  val root = Seq("")

  private val lock = new Object
  private var cxn: Client = null
  private var reg: RegistryServerProxy = null
  private var hasGetAsString: Boolean = false

  /**
   * Connect to remote registry if we are not currently connected,
   * and listener for disconnection events so we can reconnect later.
   */
  private def connect(): Unit = {
    lock.synchronized {
      if (reg == null) {
        val cxn = new Client(
          name = "Registry Proxy",
          host = host,
          port = port,
          credential = credential,
          tls = tls
        )
        val id = 123456L
        cxn.addConnectionListener { case false =>
          lock.synchronized {
            reg = null
          }
        }
        cxn.addMessageListener {
          case Message(_, _, `id`, data) =>
            val (path, name, isDir, addOrChange) = data.get[(Seq[String], String, Boolean, Boolean)]
            notifyListener(path, name, isDir, addOrChange)
        }
        cxn.connect()
        val registry = new RegistryServerProxy(cxn)
        Await.result(registry.streamChanges(id, true), 5.seconds)
        val manager = new ManagerServerProxy(cxn)
        val registrySettings = Await.result(manager.settings(Registry.NAME), 5.seconds)
        this.cxn = cxn
        this.reg = registry
        this.hasGetAsString = registrySettings.map(_._2).toSet.contains("get as string")
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

  def childImpl(dir: Dir, name: String, create: Boolean): (Dir, Boolean) = {
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

  def rmDirImpl(dir: Dir, name: String): Unit = {
    call(dir) { _.rmDir(name) }
  }

  def getValue(dir: Dir, key: String, default: Option[(Boolean, Data)]): (Data, Option[String]) = {
    call(dir) { p =>
      // We need an execution context to map over futures; use our client's execution context.
      implicit val executionContext = cxn.executionContext
      if (hasGetAsString) {
        p.getAsString(key, default = default).map { case (data, text) => (data, Some(text)) }
      } else {
        p.get(key, default = default).map { case data => (data, None) }
      }
    }
  }

  def setValueImpl(dir: Dir, key: String, value: Data, textOpt: Option[String]): Unit = {
    textOpt match {
      case Some(text) =>
        call(dir) { p =>
          if (hasGetAsString) {
            p.setAsString(key, text)
          } else {
            p.set(key, value)
          }
        }

      case None =>
        call(dir) { _.set(key, value) }
    }
  }

  def deleteImpl(dir: Dir, key: String): Unit = {
    call(dir) { _.del(key) }
  }


  // Override methods that normally result in notifications being sent.
  // We do not need to send notification because we are streaming changes from
  // the remote registry, so we will get notifications via that mechanism
  // instead.

  override def child(dir: Dir, name: String, create: Boolean): Dir = {
    childImpl(dir, name, create)._1
  }

  override def rmDir(dir: Dir, name: String): Unit = {
    rmDirImpl(dir, name)
  }

  override def setValue(dir: Dir, key: String, value: Data, textOpt: Option[String]): Unit = {
    setValueImpl(dir, key, value, textOpt)
  }

  override def delete(dir: Dir, key: String): Unit = {
    deleteImpl(dir, key)
  }
}
