package org.labrad.manager

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
}
