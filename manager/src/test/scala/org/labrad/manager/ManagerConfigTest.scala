package org.labrad.manager

import java.io.File
import java.net.URI
import org.labrad.crypto.CertConfig
import org.labrad.util.cli.Environment
import org.scalatest.FunSuite

class ManagerConfigTest extends FunSuite {

  // If not otherwise specified, use empty environment for these tests.
  implicit val defaultEnv = Environment()

  test("default manager config works with empty command line and env") {
    ManagerConfig.fromCommandLine(Array())
  }

  test("port can be set from environment variable") {
    val env = Environment("LABRADPORT" -> "7777")
    val config = ManagerConfig.fromCommandLine(Array())(env)
    assert(config.port == 7777)
  }

  test("port can be set from command line") {
    val config1 = ManagerConfig.fromCommandLine(Array("--port", "7777"))
    assert(config1.port == 7777)

    val config2 = ManagerConfig.fromCommandLine(Array("--port=7878"))
    assert(config2.port == 7878)
  }

  test("port command line arg overrides environment variable") {
    val env = Environment("LABRADPORT" -> "2345")
    val config = ManagerConfig.fromCommandLine(Array("--port=1234"))(env)
    assert(config.port == 1234)
  }

  test("port env var must be an integer") {
    val env = Environment("LABRADPORT" -> "foo")
    intercept[Exception] {
      ManagerConfig.fromCommandLine(Array())(env)
    }
  }

  test("port command line must be an integer") {
    intercept[Exception] {
      ManagerConfig.fromCommandLine(Array("--port=foo"))
    }
  }


  private def checkPassword(config: ManagerConfig, expected: String): Unit = {
    assert(config.password.toSeq == expected.toCharArray.toSeq)
  }

  test("password can be set from environment variable") {
    val env = Environment("LABRADPASSWORD" -> "foo")
    val config = ManagerConfig.fromCommandLine(Array())(env)
    checkPassword(config, "foo")
  }

  test("password can be set from command line") {
    val config1 = ManagerConfig.fromCommandLine(Array("--password", "foo"))
    checkPassword(config1, "foo")

    val config2 = ManagerConfig.fromCommandLine(Array("--password=foo"))
    checkPassword(config2, "foo")
  }

  test("password command line arg overrides environment variable") {
    val env = Environment("LABRADPASSWORD" -> "bar")
    val config = ManagerConfig.fromCommandLine(Array("--password=foo"))(env)
    checkPassword(config, "foo")
  }


  private def checkRegistry(config: ManagerConfig, expected: String): Unit = {
    assert(config.registryUri == new URI(expected))
  }

  test("registry can be set from environment variable") {
    val env = Environment("LABRADREGISTRY" -> "/var/labrad/registry/")
    val config = ManagerConfig.fromCommandLine(Array())(env)
    checkRegistry(config, "/var/labrad/registry/")
  }

  test("registry can be set from command line") {
    val config1 = ManagerConfig.fromCommandLine(Array("--registry", "/var/labrad/registry/"))
    checkRegistry(config1, "/var/labrad/registry/")

    val config2 = ManagerConfig.fromCommandLine(Array("--registry=/var/labrad/registry/"))
    checkRegistry(config2, "/var/labrad/registry/")
  }

  test("registry command line arg overrides environment variable") {
    val env = Environment("LABRADREGISTRY" -> "/var/labrad/fake-reg")
    val config = ManagerConfig.fromCommandLine(Array("--registry=/var/labrad/real-reg/"))(env)
    checkRegistry(config, "/var/labrad/real-reg/")
  }


  test("tls-port can be set from environment variable") {
    val env = Environment("LABRAD_TLS_PORT" -> "7777")
    val config = ManagerConfig.fromCommandLine(Array())(env)
    assert(config.tlsPort == 7777)
  }

  test("tls-port can be set from command line") {
    val config1 = ManagerConfig.fromCommandLine(Array("--tls-port", "7777"))
    assert(config1.tlsPort == 7777)

    val config2 = ManagerConfig.fromCommandLine(Array("--tls-port=7878"))
    assert(config2.tlsPort == 7878)
  }

  test("tls-port command line arg overrides environment variable") {
    val env = Environment("LABRAD_TLS_PORT" -> "2345")
    val config = ManagerConfig.fromCommandLine(Array("--tls-port=1234"))(env)
    assert(config.tlsPort == 1234)
  }

  test("tls-port env var must be an integer") {
    val env = Environment("LABRAD_TLS_PORT" -> "foo")
    intercept[Exception] {
      ManagerConfig.fromCommandLine(Array())(env)
    }
  }

  test("tls-port command line must be an integer") {
    intercept[Exception] {
      ManagerConfig.fromCommandLine(Array("--tls-port=foo"))
    }
  }


  test("tls-required can be set from environment variable") {
    val env = Environment("LABRAD_TLS_REQUIRED" -> "no")
    val config = ManagerConfig.fromCommandLine(Array())(env)
    assert(config.tlsRequired == false)
  }

  test("tls-required can be set from command line") {
    val config1 = ManagerConfig.fromCommandLine(Array("--tls-required", "no"))
    assert(config1.tlsRequired == false)

    val config2 = ManagerConfig.fromCommandLine(Array("--tls-required=no"))
    assert(config2.tlsRequired == false)
  }

  test("tls-required command line arg overrides environment variable") {
    val env = Environment("LABRAD_TLS_REQUIRED" -> "yes")
    val config = ManagerConfig.fromCommandLine(Array("--tls-required=no"))(env)
    assert(config.tlsRequired == false)
  }

  test("tls-required env var must be a valid option") {
    val env = Environment("LABRAD_TLS_REQUIRED" -> "foo")
    intercept[Exception] {
      ManagerConfig.fromCommandLine(Array())(env)
    }
  }

  test("tls-required command line must be a valid option") {
    intercept[Exception] {
      ManagerConfig.fromCommandLine(Array("--tls-required=foo"))
    }
  }


  test("tls-hosts can be set from environment variable") {
    val env = Environment("LABRAD_TLS_HOSTS" -> "foo.com")
    val config = ManagerConfig.fromCommandLine(Array())(env)
    assert(config.tlsHosts == Map("foo.com" -> CertConfig.SelfSigned))
  }

  test("tls-hosts can be set from command line") {
    val config1 = ManagerConfig.fromCommandLine(Array("--tls-hosts", "foo.com"))
    assert(config1.tlsHosts == Map("foo.com" -> CertConfig.SelfSigned))

    val config2 = ManagerConfig.fromCommandLine(Array("--tls-hosts=foo.com"))
    assert(config2.tlsHosts == Map("foo.com" -> CertConfig.SelfSigned))
  }

  test("tls-hosts command line arg overrides environment variable") {
    val env = Environment("LABRAD_TLS_HOSTS" -> "bar.com")
    val config = ManagerConfig.fromCommandLine(Array("--tls-hosts=foo.com"))(env)
    assert(config.tlsHosts == Map("foo.com" -> CertConfig.SelfSigned))
  }

  test("tls-hosts may contain cert and key files") {
    val config1 = ManagerConfig.fromCommandLine(Array(
      "--tls-hosts=foo.com?cert=/my/cert/file.crt&key=/my/key/file.pem"))
    assert(config1.tlsHosts == Map(
      "foo.com" -> CertConfig.Files(
        cert = new File("/my/cert/file.crt"),
        key = new File("/my/key/file.pem"))))

    val config2 = ManagerConfig.fromCommandLine(Array(
      "--tls-hosts=foo.com?cert=/my/cert/file.crt&key=/my/key/file.pem&intermediates=/my/interm/file.pem"))
    assert(config2.tlsHosts == Map(
      "foo.com" -> CertConfig.Files(
        cert = new File("/my/cert/file.crt"),
        key = new File("/my/key/file.pem"),
        intermediates = Some(new File("/my/interm/file.pem")))))
  }

  test("tls-hosts must provide cert and key if either is provided") {
    intercept[Exception] {
      ManagerConfig.fromCommandLine(Array("--tls-hosts=foo.com?cert=/my/cert/file.crt"))
    }

    intercept[Exception] {
      ManagerConfig.fromCommandLine(Array("--tls-hosts=foo.com?key=/my/key/file.pem"))
    }
  }

  test("tls-hosts may contain multiple semicolon-separated hosts") {
    val config = ManagerConfig.fromCommandLine(Array(
      "--tls-hosts=private1;public.com?cert=/my/cert/file.crt&key=/my/key/file.pem;private2"))
    assert(config.tlsHosts == Map(
      "private1" -> CertConfig.SelfSigned,
      "private2" -> CertConfig.SelfSigned,
      "public.com" -> CertConfig.Files(
        cert = new File("/my/cert/file.crt"),
        key = new File("/my/key/file.pem"))))
  }
}
