package org.labrad

import java.net.URI
import org.scalatest.FunSuite
import scala.util.{Failure, Success}

class ServerConfigTest extends FunSuite {

  test("default manager config works with empty command line and env") {
    ServerConfig.fromCommandLine(Array())
  }

  test("host can be set from environment variable") {
    val config = ServerConfig.fromCommandLine(
      Array(),
      env = Map("LABRADHOST" -> "foo.com"))
    assert(config.host == "foo.com")
  }

  test("host can be set from command line") {
    val config1 = ServerConfig.fromCommandLine(Array("--host", "foo.com"))
    assert(config1.host == "foo.com")

    val config2 = ServerConfig.fromCommandLine(Array("--host=foo.com"))
    assert(config2.host == "foo.com")
  }

  test("host command line arg overrides environment variable") {
    val config = ServerConfig.fromCommandLine(
      Array("--host=foo.com"),
      env = Map("LABRADHOST" -> "bar.com"))
    assert(config.host == "foo.com")
  }

  test("port can be set from environment variable") {
    val config = ServerConfig.fromCommandLine(Array(), env = Map("LABRADPORT" -> "7777"))
    assert(config.port == 7777)
  }

  test("port can be set from command line") {
    val config1 = ServerConfig.fromCommandLine(Array("--port", "7777"))
    assert(config1.port == 7777)

    val config2 = ServerConfig.fromCommandLine(Array("--port=7878"))
    assert(config2.port == 7878)
  }

  test("port command line arg overrides environment variable") {
    val config = ServerConfig.fromCommandLine(
      Array("--port=1234"),
      env = Map("LABRADPORT" -> "2345"))
    assert(config.port == 1234)
  }

  test("port env var must be an integer") {
    intercept[Exception] {
      ServerConfig.fromCommandLine(Array(), env = Map("LABRADPORT" -> "foo"))
    }
  }

  test("port command line must be an integer") {
    intercept[Exception] {
      ServerConfig.fromCommandLine(Array("--port=foo"))
    }
  }


  test("username can be set from environment variable") {
    val config = ServerConfig.fromCommandLine(
      Array(),
      env = Map("LABRADUSER" -> "foo"))
    val Password(username, _) = config.credential
    assert(username == "foo")
  }

  test("username can be set from command line") {
    val config1 = ServerConfig.fromCommandLine(Array("--username", "foo"))
    val Password(username1, _) = config1.credential
    assert(username1 == "foo")

    val config2 = ServerConfig.fromCommandLine(Array("--username=foo"))
    val Password(username2, _) = config2.credential
    assert(username2 == "foo")
  }

  test("username command line arg overrides environment variable") {
    val config = ServerConfig.fromCommandLine(
      Array("--username=foo"),
      env = Map("LABRADUSER" -> "bar"))
    val Password(username, _) = config.credential
    assert(username == "foo")
  }


  private def checkPassword(config: ServerConfig, expected: String): Unit = {
    val Password(_, password) = config.credential
    assert(password.toSeq == expected.toCharArray.toSeq)
  }

  test("password can be set from environment variable") {
    val config = ServerConfig.fromCommandLine(
      Array(),
      env = Map("LABRADPASSWORD" -> "foo"))
    checkPassword(config, "foo")
  }

  test("password can be set from command line") {
    val config1 = ServerConfig.fromCommandLine(Array("--password", "foo"))
    checkPassword(config1, "foo")

    val config2 = ServerConfig.fromCommandLine(Array("--password=foo"))
    checkPassword(config2, "foo")
  }

  test("password command line arg overrides environment variable") {
    val config = ServerConfig.fromCommandLine(
      Array("--password=foo"),
      env = Map("LABRADPASSWORD" -> "bar"))
    checkPassword(config, "foo")
  }


  test("server name can be set from command line") {
    val config1 = ServerConfig.fromCommandLine(Array("--name", "foo-server"))
    assert(config1.nameOpt == Some("foo-server"))

    val config2 = ServerConfig.fromCommandLine(Array("--name=foo-server"))
    assert(config2.nameOpt == Some("foo-server"))
  }

  test("server name defaults to None") {
    val config = ServerConfig.fromCommandLine(Array())
    assert(config.nameOpt == None)
  }


  test("tls-port can be set from environment variable") {
    val config = ServerConfig.fromCommandLine(Array(), env = Map("LABRAD_TLS_PORT" -> "7777"))
    assert(config.tlsPort == 7777)
  }

  test("tls-port can be set from command line") {
    val config1 = ServerConfig.fromCommandLine(Array("--tls-port", "7777"))
    assert(config1.tlsPort == 7777)

    val config2 = ServerConfig.fromCommandLine(Array("--tls-port=7878"))
    assert(config2.tlsPort == 7878)
  }

  test("tls-port command line arg overrides environment variable") {
    val config = ServerConfig.fromCommandLine(
      Array("--tls-port=1234"),
      env = Map("LABRAD_TLS_PORT" -> "2345"))
    assert(config.tlsPort == 1234)
  }

  test("tls-port env var must be an integer") {
    intercept[Exception] {
      ServerConfig.fromCommandLine(Array(), env = Map("LABRAD_TLS_PORT" -> "foo"))
    }
  }

  test("tls-port command line must be an integer") {
    intercept[Exception] {
      ServerConfig.fromCommandLine(Array("--tls-port=foo"))
    }
  }


  test("tls mode can be set from environment variable") {
    val config = ServerConfig.fromCommandLine(Array(), env = Map("LABRAD_TLS" -> "on"))
    assert(config.tls == TlsMode.ON)
  }

  test("tls mode can be set from command line") {
    val config1 = ServerConfig.fromCommandLine(Array("--tls", "on"))
    assert(config1.tls == TlsMode.ON)

    val config2 = ServerConfig.fromCommandLine(Array("--tls=on"))
    assert(config2.tls == TlsMode.ON)
  }

  test("tls mode command line arg overrides environment variable") {
    val config = ServerConfig.fromCommandLine(
      Array("--tls=off"),
      env = Map("LABRAD_TLS" -> "on"))
    assert(config.tls == TlsMode.OFF)
  }

  test("tls mode env var must be a valid option") {
    intercept[Exception] {
      ServerConfig.fromCommandLine(Array(), env = Map("LABRAD_TLS" -> "foo"))
    }
  }

  test("tls mode command line must be a valid option") {
    intercept[Exception] {
      ServerConfig.fromCommandLine(Array("--tls=foo"))
    }
  }
}
