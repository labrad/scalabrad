package org.labrad.manager

import java.io.File
import java.net.URI
import org.scalatest.FunSuite
import scala.util.{Failure, Success}

class ManagerConfigTest extends FunSuite {

  test("default manager config works with empty command line and env") {
    val tryConfig = ManagerConfig.fromCommandLine(Array())
    assert(tryConfig.isInstanceOf[Success[_]])
  }

  test("port can be set from environment variable") {
    val Success(config) = ManagerConfig.fromCommandLine(Array(), env = Map("LABRADPORT" -> "7777"))
    assert(config.port == 7777)
  }

  test("port can be set from command line") {
    val Success(config1) = ManagerConfig.fromCommandLine(Array("--port", "7777"))
    assert(config1.port == 7777)

    val Success(config2) = ManagerConfig.fromCommandLine(Array("--port=7878"))
    assert(config2.port == 7878)
  }

  test("port command line arg overrides environment variable") {
    val Success(config) = ManagerConfig.fromCommandLine(
      Array("--port=1234"),
      env = Map("LABRADPORT" -> "2345"))
    assert(config.port == 1234)
  }

  test("port env var must be an integer") {
    val tryConfig = ManagerConfig.fromCommandLine(Array(), env = Map("LABRADPORT" -> "foo"))
    assert(tryConfig.isInstanceOf[Failure[_]])
  }

  test("port command line must be an integer") {
    val tryConfig = ManagerConfig.fromCommandLine(Array("--port=foo"))
    assert(tryConfig.isInstanceOf[Failure[_]])
  }


  private def checkPassword(config: ManagerConfig, expected: String): Unit = {
    assert(config.password.toSeq == expected.toCharArray.toSeq)
  }

  test("password can be set from environment variable") {
    val Success(config) = ManagerConfig.fromCommandLine(
      Array(),
      env = Map("LABRADPASSWORD" -> "foo"))
    checkPassword(config, "foo")
  }

  test("password can be set from command line") {
    val Success(config1) = ManagerConfig.fromCommandLine(Array("--password", "foo"))
    checkPassword(config1, "foo")

    val Success(config2) = ManagerConfig.fromCommandLine(Array("--password=foo"))
    checkPassword(config2, "foo")
  }

  test("password command line arg overrides environment variable") {
    val Success(config) = ManagerConfig.fromCommandLine(
      Array("--password=foo"),
      env = Map("LABRADPASSWORD" -> "bar"))
    checkPassword(config, "foo")
  }


  private def checkRegistry(config: ManagerConfig, expected: String): Unit = {
    assert(config.registryUri == new URI(expected))
  }

  test("registry can be set from environment variable") {
    val Success(config) = ManagerConfig.fromCommandLine(
      Array(),
      env = Map("LABRADREGISTRY" -> "/var/labrad/registry/"))
    checkRegistry(config, "/var/labrad/registry/")
  }

  test("registry can be set from command line") {
    val Success(config1) = ManagerConfig.fromCommandLine(
      Array("--registry", "/var/labrad/registry/"))
    checkRegistry(config1, "/var/labrad/registry/")

    val Success(config2) = ManagerConfig.fromCommandLine(
      Array("--registry=/var/labrad/registry/"))
    checkRegistry(config2, "/var/labrad/registry/")
  }

  test("registry command line arg overrides environment variable") {
    val Success(config) = ManagerConfig.fromCommandLine(
      Array("--registry=/var/labrad/registry/for-real/"),
      env = Map("LABRADREGISTRY" -> "/var/labrad/registry/not-this-time"))
    checkRegistry(config, "/var/labrad/registry/for-real/")
  }


  test("tls-port can be set from environment variable") {
    val Success(config) = ManagerConfig.fromCommandLine(Array(), env = Map("LABRAD_TLS_PORT" -> "7777"))
    assert(config.tlsPort == 7777)
  }

  test("tls-port can be set from command line") {
    val Success(config1) = ManagerConfig.fromCommandLine(Array("--tls-port", "7777"))
    assert(config1.tlsPort == 7777)

    val Success(config2) = ManagerConfig.fromCommandLine(Array("--tls-port=7878"))
    assert(config2.tlsPort == 7878)
  }

  test("tls-port command line arg overrides environment variable") {
    val Success(config) = ManagerConfig.fromCommandLine(
      Array("--tls-port=1234"),
      env = Map("LABRAD_TLS_PORT" -> "2345"))
    assert(config.tlsPort == 1234)
  }

  test("tls-port env var must be an integer") {
    val tryConfig = ManagerConfig.fromCommandLine(Array(), env = Map("LABRAD_TLS_PORT" -> "foo"))
    assert(tryConfig.isInstanceOf[Failure[_]])
  }

  test("tls-port command line must be an integer") {
    val tryConfig = ManagerConfig.fromCommandLine(Array("--tls-port=foo"))
    assert(tryConfig.isInstanceOf[Failure[_]])
  }


  test("tls-required can be set from environment variable") {
    val Success(config) = ManagerConfig.fromCommandLine(Array(), env = Map("LABRAD_TLS_REQUIRED" -> "no"))
    assert(config.tlsRequired == false)
  }

  test("tls-required can be set from command line") {
    val Success(config1) = ManagerConfig.fromCommandLine(Array("--tls-required", "no"))
    assert(config1.tlsRequired == false)

    val Success(config2) = ManagerConfig.fromCommandLine(Array("--tls-required=no"))
    assert(config2.tlsRequired == false)
  }

  test("tls-required command line arg overrides environment variable") {
    val Success(config) = ManagerConfig.fromCommandLine(
      Array("--tls-required=no"),
      env = Map("LABRAD_TLS_REQUIRED" -> "yes"))
    assert(config.tlsRequired == false)
  }

  test("tls-required env var must be a valid option") {
    val tryConfig = ManagerConfig.fromCommandLine(Array(), env = Map("LABRAD_TLS_REQUIRED" -> "foo"))
    assert(tryConfig.isInstanceOf[Failure[_]])
  }

  test("tls-required command line must be a valid option") {
    val tryConfig = ManagerConfig.fromCommandLine(Array("--tls-required=foo"))
    assert(tryConfig.isInstanceOf[Failure[_]])
  }


  test("tls-hosts can be set from environment variable") {
    val Success(config) = ManagerConfig.fromCommandLine(Array(), env = Map("LABRAD_TLS_HOSTS" -> "foo.com"))
    assert(config.tlsHosts == Map("foo.com" -> CertConfig.SelfSigned))
  }

  test("tls-hosts can be set from command line") {
    val Success(config1) = ManagerConfig.fromCommandLine(Array("--tls-hosts", "foo.com"))
    assert(config1.tlsHosts == Map("foo.com" -> CertConfig.SelfSigned))

    val Success(config2) = ManagerConfig.fromCommandLine(Array("--tls-hosts=foo.com"))
    assert(config2.tlsHosts == Map("foo.com" -> CertConfig.SelfSigned))
  }

  test("tls-hosts command line arg overrides environment variable") {
    val Success(config) = ManagerConfig.fromCommandLine(
      Array("--tls-hosts=foo.com"),
      env = Map("LABRAD_TLS_HOSTS" -> "bar.com"))
    assert(config.tlsHosts == Map("foo.com" -> CertConfig.SelfSigned))
  }

  test("tls-hosts may contain cert and key files") {
    val Success(config1) = ManagerConfig.fromCommandLine(Array(
      "--tls-hosts=foo.com?cert=/my/cert/file.crt&key=/my/key/file.pem"))
    assert(config1.tlsHosts == Map(
      "foo.com" -> CertConfig.Files(
        cert = new File("/my/cert/file.crt"),
        key = new File("/my/key/file.pem"))))

    val Success(config2) = ManagerConfig.fromCommandLine(Array(
      "--tls-hosts=foo.com?cert=/my/cert/file.crt&key=/my/key/file.pem&intermediates=/my/interm/file.pem"))
    assert(config2.tlsHosts == Map(
      "foo.com" -> CertConfig.Files(
        cert = new File("/my/cert/file.crt"),
        key = new File("/my/key/file.pem"),
        intermediates = Some(new File("/my/interm/file.pem")))))
  }

  test("tls-hosts must provide cert and key if either is provided") {
    val tryConfig1 = ManagerConfig.fromCommandLine(Array("--tls-hosts=foo.com?cert=/my/cert/file.crt"))
    assert(tryConfig1.isInstanceOf[Failure[_]])

    val tryConfig2 = ManagerConfig.fromCommandLine(Array("--tls-hosts=foo.com?key=/my/key/file.pem"))
    assert(tryConfig2.isInstanceOf[Failure[_]])
  }

  test("tls-hosts may contain multiple semicolon-separated hosts") {
    val Success(config) = ManagerConfig.fromCommandLine(Array(
      "--tls-hosts=private1;public.com?cert=/my/cert/file.crt&key=/my/key/file.pem;private2"))
    assert(config.tlsHosts == Map(
      "private1" -> CertConfig.SelfSigned,
      "private2" -> CertConfig.SelfSigned,
      "public.com" -> CertConfig.Files(
        cert = new File("/my/cert/file.crt"),
        key = new File("/my/key/file.pem"))))
  }
}
