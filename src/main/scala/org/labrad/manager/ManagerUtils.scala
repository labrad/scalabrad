package org.labrad.manager

import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.util.SelfSignedCertificate
import java.io.File
import org.labrad.{Client, Credential, Password, TlsMode}
import org.labrad.registry._
import org.labrad.util.Files
import scala.concurrent.duration._

object ManagerUtils {

  case class ManagerInfo(
    host: String,
    port: Int,
    credential: Credential,
    tlsPolicy: TlsPolicy,
    tlsHosts: TlsHostConfig
  )

  def withManager[T](
    tlsPolicy: TlsPolicy = TlsPolicy.OFF,
    registryStore: Option[RegistryStore] = None
  )(f: ManagerInfo => T): T = {
    val host = "localhost"
    val port = 0 // OS will choose a free port for us.

    val password = "testPassword12345!@#$%".toCharArray
    val credential = Password("", password)

    def run(registryStore: RegistryStore): T = {
      val ssc = new SelfSignedCertificate()
      val sslCtx = SslContextBuilder.forServer(ssc.certificate, ssc.privateKey).build()
      val tlsHosts = TlsHostConfig((ssc.certificate, sslCtx))
      val listeners = Seq(port -> tlsPolicy)

      val manager = new CentralNode(password, Some(registryStore), None, Map(), listeners, tlsHosts,
                                    authTimeout = 1.second, registryTimeout = 1.second)
      val address = manager.listener.channels.head.localAddress
      try {
        f(ManagerInfo(host, address.getPort, credential, tlsPolicy, tlsHosts))
      } finally {
        manager.stop()
      }
    }

    registryStore match {
      case Some(store) => run(store)
      case None =>
        Files.withTempDir { registryDir =>
          val registryStore = new BinaryFileStore(registryDir)
          run(registryStore)
        }
    }
  }

  def withClient[A](
    m: ManagerInfo,
    tls: TlsMode = TlsMode.OFF,
    tlsCerts: Map[String, File] = Map()
  )(func: Client => A): A = {
    val c = new Client(
      host = m.host,
      port = m.port,
      credential = m.credential,
      tls = tls,
      tlsCerts = tlsCerts
    )
    c.connect()
    try {
      func(c)
    } finally {
      try c.close() catch { case _: Throwable => }
    }
  }
}
