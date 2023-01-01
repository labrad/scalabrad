package org.labrad

import org.labrad.util.cli.Environment
import org.scalatest.FunSuite

class ServerConfigTest extends FunSuite {

  // If not otherwise specified, use empty environment for these tests.
  implicit val defaultEnv = Environment()

  test("default manager config works with empty command line and env") {
    ServerConfig.fromCommandLine(Array())
  }

  test("host can be set from environment variable") {
    val env = Environment("LABRADHOST" -> "foo.com")
    val config = ServerConfig.fromCommandLine(Array())(env)
    assert(config.host == "foo.com")
  }

  test("host can be set from command line") {
    val config1 = ServerConfig.fromCommandLine(Array("--host", "foo.com"))
    assert(config1.host == "foo.com")

    val config2 = ServerConfig.fromCommandLine(Array("--host=foo.com"))
    assert(config2.host == "foo.com")
  }

  test("host command line arg overrides environment variable") {
    val env = Environment("LABRADHOST" -> "bar.com")
    val config = ServerConfig.fromCommandLine(Array("--host=foo.com"))(env)
    assert(config.host == "foo.com")
  }

  test("port can be set from environment variable") {
    val env = Environment("LABRADPORT" -> "7777")
    val config = ServerConfig.fromCommandLine(Array())(env)
    assert(config.port == 7777)
  }

  test("port can be set from command line") {
    val config1 = ServerConfig.fromCommandLine(Array("--port", "7777"))
    assert(config1.port == 7777)

    val config2 = ServerConfig.fromCommandLine(Array("--port=7878"))
    assert(config2.port == 7878)
  }

  test("port command line arg overrides environment variable") {
    val env = Environment("LABRADPORT" -> "2345")
    val config = ServerConfig.fromCommandLine(Array("--port=1234"))(env)
    assert(config.port == 1234)
  }

  test("port env var must be an integer") {
    val env = Environment("LABRADPORT" -> "foo")
    intercept[Exception] {
      ServerConfig.fromCommandLine(Array())(env)
    }
  }

  test("port command line must be an integer") {
    intercept[Exception] {
      ServerConfig.fromCommandLine(Array("--port=foo"))
    }
  }


  test("username can be set from environment variable") {
    val env = Environment("LABRADUSER" -> "foo")
    val config = ServerConfig.fromCommandLine(Array())(env)
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
    val env = Environment("LABRADUSER" -> "bar")
    val config = ServerConfig.fromCommandLine(Array("--username=foo"))(env)
    val Password(username, _) = config.credential
    assert(username == "foo")
  }


  private def checkPassword(config: ServerConfig, expected: String): Unit = {
    val Password(_, password) = config.credential
    assert(password.toSeq == expected.toCharArray.toSeq)
  }

  test("password can be set from environment variable") {
    val env = Environment("LABRADPASSWORD" -> "foo")
    val config = ServerConfig.fromCommandLine(Array())(env)
    checkPassword(config, "foo")
  }

  test("password can be set from command line") {
    val config1 = ServerConfig.fromCommandLine(Array("--password", "foo"))
    checkPassword(config1, "foo")

    val config2 = ServerConfig.fromCommandLine(Array("--password=foo"))
    checkPassword(config2, "foo")
  }

  test("password command line arg overrides environment variable") {
    val env = Environment("LABRADPASSWORD" -> "bar")
    val config = ServerConfig.fromCommandLine(Array("--password=foo"))(env)
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
    val env = Environment("LABRAD_TLS_PORT" -> "7777")
    val config = ServerConfig.fromCommandLine(Array())(env)
    assert(config.tlsPort == 7777)
  }

  test("tls-port can be set from command line") {
    val config1 = ServerConfig.fromCommandLine(Array("--tls-port", "7777"))
    assert(config1.tlsPort == 7777)

    val config2 = ServerConfig.fromCommandLine(Array("--tls-port=7878"))
    assert(config2.tlsPort == 7878)
  }

  test("tls-port command line arg overrides environment variable") {
    val env = Environment("LABRAD_TLS_PORT" -> "2345")
    val config = ServerConfig.fromCommandLine(Array("--tls-port=1234"))(env)
    assert(config.tlsPort == 1234)
  }

  test("tls-port env var must be an integer") {
    val env = Environment("LABRAD_TLS_PORT" -> "foo")
    intercept[Exception] {
      ServerConfig.fromCommandLine(Array())(env)
    }
  }

  test("tls-port command line must be an integer") {
    intercept[Exception] {
      ServerConfig.fromCommandLine(Array("--tls-port=foo"))
    }
  }


  test("tls mode can be set from environment variable") {
    val env = Environment("LABRAD_TLS" -> "on")
    val config = ServerConfig.fromCommandLine(Array())(env)
    assert(config.tls == TlsMode.ON)
  }

  test("tls mode can be set from command line") {
    val config1 = ServerConfig.fromCommandLine(Array("--tls", "on"))
    assert(config1.tls == TlsMode.ON)

    val config2 = ServerConfig.fromCommandLine(Array("--tls=on"))
    assert(config2.tls == TlsMode.ON)
  }

  test("tls mode command line arg overrides environment variable") {
    val env = Environment("LABRAD_TLS" -> "on")
    val config = ServerConfig.fromCommandLine(Array("--tls=off"))(env)
    assert(config.tls == TlsMode.OFF)
  }

  test("tls mode env var must be a valid option") {
    val env = Environment("LABRAD_TLS" -> "foo")
    intercept[Exception] {
      ServerConfig.fromCommandLine(Array())(env)
    }
  }

  test("tls mode command line must be a valid option") {
    intercept[Exception] {
      ServerConfig.fromCommandLine(Array("--tls=foo"))
    }
  }
}
