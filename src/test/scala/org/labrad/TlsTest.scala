package org.labrad

import java.io.File
import org.labrad.data._
import org.labrad.manager.TlsPolicy
import org.labrad.types._
import org.scalatest.FunSuite
import scala.concurrent.Await
import scala.concurrent.duration._

class TlsTest extends FunSuite {

  import TestUtils._

  def connectWithMode(tls: TlsMode, includeCerts: Boolean = true)(implicit m: ManagerInfo): Unit = {
    val certs = if (includeCerts) {
      Map(m.host -> m.tlsHosts.certFiles.map(m.host))
    } else {
      Map.empty[String, File]
    }
    withClient(m.host, m.port, m.password, tls = tls, tlsCerts = certs) { c =>
      val mgr = new ManagerServerProxy(c)
      Await.result(mgr.servers(), 1.second)
    }
  }

  test("when manager TLS is ON, initial client connection must use TLS") {
    withManager(TlsPolicy.ON) { implicit m =>
      intercept[Exception] { connectWithMode(TlsMode.OFF) }
      intercept[Exception] { connectWithMode(TlsMode.STARTTLS) }
      intercept[Exception] { connectWithMode(TlsMode.STARTTLS_FORCE) }
      connectWithMode(TlsMode.ON)
    }
  }

  test("when manager TLS is OFF, client connection must not use TLS or STARTTLS") {
    withManager(TlsPolicy.OFF) { implicit m =>
      connectWithMode(TlsMode.OFF)
      connectWithMode(TlsMode.STARTTLS) // localhost; will not send STARTTLS
      intercept[Exception] { connectWithMode(TlsMode.STARTTLS_FORCE) }
      intercept[Exception] { connectWithMode(TlsMode.ON) }
    }
  }

  test("when manager TLS is STARTTLS, client connection may use STARTTLS") {
    withManager(TlsPolicy.STARTTLS) { implicit m =>
      connectWithMode(TlsMode.OFF)
      connectWithMode(TlsMode.STARTTLS)
      connectWithMode(TlsMode.STARTTLS_FORCE)
      intercept[Exception] { connectWithMode(TlsMode.ON) }
    }
  }

  test("when manager TLS is STARTTLS_OPT, client connection may use STARTTLS") {
    withManager(TlsPolicy.STARTTLS_OPT) { implicit m =>
      connectWithMode(TlsMode.OFF)
      connectWithMode(TlsMode.STARTTLS) // localhost; will not actually STARTTLS
      connectWithMode(TlsMode.STARTTLS_FORCE)
      intercept[Exception] { connectWithMode(TlsMode.ON) }
    }
  }

  test("when manager TLS is STARTTLS_FORCE, client connection must use STARTTLS") {
    withManager(TlsPolicy.STARTTLS_FORCE) { implicit m =>
      intercept[Exception] { connectWithMode(TlsMode.OFF) }
      intercept[Exception] { connectWithMode(TlsMode.STARTTLS) }
      connectWithMode(TlsMode.STARTTLS_FORCE)
      intercept[Exception] { connectWithMode(TlsMode.ON) }
    }
  }

  test("client fails to connect if manager cert is not trusted") {
    withManager(TlsPolicy.ON) { implicit m =>
      intercept[Exception] { connectWithMode(TlsMode.ON, includeCerts = false) }
    }
    withManager(TlsPolicy.STARTTLS_FORCE) { implicit m =>
      intercept[Exception] { connectWithMode(TlsMode.STARTTLS_FORCE, includeCerts = false) }
    }
  }
}
