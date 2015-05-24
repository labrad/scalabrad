package org.labrad

import org.labrad.annotations._
import org.labrad.data._
import org.labrad.util.Logging
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.fixture.FunSuite
import scala.collection._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.reflect.runtime.universe

@IsServer(name = "Scala Test Server",
           doc = "Basic server to test Scalabrad API.")
class TestSrv extends Server[TestSrv, TestCtx] with Logging {
  def init(cxn: ServerConnection): Unit = log.info("init() called on server.")
  def shutdown(): Unit = log.info("shutdown() called on server.")

  @Setting(id = 100, name = "Srv Echo", doc = "setting defined on server")
  def serverEcho(data: Data): Data = {
    log.debug(s"Echo: $data")
    data
  }
}


class TestCtx(cxn: ServerConnection, server: TestSrv, context: Context)
extends ServerContext(cxn, server, context) with Logging {
  private val registry = mutable.Map.empty[String, Data]

  def init(): Unit = {
    registry("Test") = Str("blah")
    log.debug(s"Context $context created")
  }

  def expire(): Unit = {
    log.debug(s"Context $context expired")
  }

  private def makeRequest(server: String, setting: String, data: Data = Data.NONE) =
    Await.result(cxn.send(server, context, setting -> data), 10.seconds)(0)


  //
  // Settings
  //

  @Setting(id = 1,
           name = "Echo",
           doc = "Echoes back any data sent to this setting.")
  def echo(data: Data): Data = {
    log.debug(s"Echo: $data")
    data
  }

  @Setting(id = 2,
           name = "Delayed Echo",
           doc = "Echoes back data after a specified delay.")
  def delayedEcho(@Accept("v[s]") delay: Double, payload: Data): Data = {
    log.debug(s"Delayed Echo ($delay seconds): $payload")
    Thread.sleep((delay*1000).toLong)
    payload
  }

  @Setting(id = 3,
           name = "Set",
           doc = "Sets a key value pair in the current context.")
  def set(key: String, value: Data): Data = {
    log.debug(s"Set: $key = $value")
    registry(key) = value
    value
  }

  @Setting(id = 4,
           name = "Get",
           doc = "Gets a key from the current context.")
  def get(key: String): Data = {
    log.debug(s"Get: $key")
    registry.get(key) match {
      case Some(value) => value
      case None => sys.error(s"Invalid key: $key")
    }
  }

  @Setting(id = 5,
           name = "Get All",
           doc = "Gets all of the key-value pairs defined in this context.")
  def getAll(): Seq[(String, Data)] = {
    log.debug("Get All")
    registry.toSeq.sortBy(_._1)
  }

  @Setting(id = 6,
           name = "Keys",
           doc = "Returns a list of all keys defined in this context.")
  def getKeys(): Seq[String] = {
    log.debug("Keys")
    registry.keys.toSeq.sorted
  }

  @Setting(id = 7,
           name = "Remove",
           doc = "Removes the specified key from this context.")
  def remove(key: String): Unit = {
    log.debug(s"Remove: $key")
    registry -= key
  }

  @Setting(id = 8,
           name = "Get Random Data",
           doc = """Returns random LabRAD data.

                   |If a type is specified, the data will be of that type;
                   |otherwise it will be of a random type.""")
  def getRandomData(typ: String): Data = {
    log.debug(s"Get Random Data: $typ")
    Hydrant.randomData(typ)
  }
  def getRandomData(): Data = {
    log.debug("Get Random Data (no type)")
    Hydrant.randomData
  }

  @Setting(id = 9,
           name = "Get Random Data Remote",
           doc = "Fetches random data by making a request to the python test server.")
  def getRandomDataRemote(typ: Option[String] = None): Data = typ match {
    case Some(typ) =>
      log.debug("Get Random Data Remote: $typ")
      makeRequest("Python Test Server", "Get Random Data", Str(typ))

    case None =>
      log.debug("Get Random Data Remote (no type)")
      makeRequest("Python Test Server", "Get Random Data")
  }

  @Setting(id = 10,
           name = "Forward Request",
           doc = "Forwards a request on to another server, specified by name and setting.")
  def forwardRequest(server: String, setting: String, payload: Data): Data = {
    log.debug("Forward Request: server='$server', setting='$setting', payload=$payload")
    makeRequest(server, setting, payload)
  }

  @Setting(id = 11,
           name = "Test No Args",
           doc = "Test setting that takes no arguments.")
  def noArgs(): Boolean = {
    log.debug("Test No Args")
    true
  }

  @Setting(id = 12,
           name = "Test No Return",
           doc = "Test setting with no return value.")
  def noReturn(data: Data): Unit = {
    log.debug(s"Test No Return: $data")
  }

  @Setting(id = 13,
           name = "Test No Args No Return",
           doc = "Test setting that takes no arguments and has no return value.")
  def noArgsNoReturn(): Unit = {
    log.debug("Test No Args No Return")
  }
}

class ServerTest extends FunSuite with AsyncAssertions {

  import TestUtils._

  type FixtureParam = Client

  def withFixture(test: OneArgTest) = {
    withManager { (host, port, password) =>
      withServer(host, port, password) {
        withClient(host, port, password) { client =>
          withFixture(test.toNoArgTest(client))
        }
      }
    }
  }

  test("server can log in and log out of manager") { c =>
    val msg = "This is a test"
    val result = await(c.send("Scala Test Server", "Echo" -> Str(msg)))
    val Str(s) = result(0)
    assert(s == msg)

    val aVal = 1
    await(c.send("Scala Test Server", "Set" -> Cluster(Str("a"), UInt(aVal))))
    val result2 = await(c.send("Scala Test Server", "Get" -> Str("a")))
    val UInt(a) = result2(0)
    assert(a == 1)
  }

  test("can call setting defined on server object") { c =>
    val msg = "This is a test"
    val result = await(c.send("Scala Test Server", "Srv Echo" -> Str(msg)))
    val Str(s) = result(0)
    assert(s == msg)
  }
}

object TestServer {
  def main(args: Array[String]): Unit = {
    def parseArgs(args: List[String]): Map[String, String] = args match {
      case Nil => Map()
      case "--host" :: host :: rest => parseArgs(rest) + ("host" -> host)
      case "--password" :: password :: rest => parseArgs(rest) + ("password" -> password)
      case "--port" :: port :: rest => parseArgs(rest) + ("port" -> port)
      case arg :: rest => sys.error("Unknown argument: " + arg)
    }
    val options = parseArgs(args.toList)

    val HOST = options.get("host").orElse(sys.env.get("LABRADHOST")).getOrElse("localhost")
    val PORT = options.get("port").orElse(sys.env.get("LABRADPORT")).map(_.toInt).getOrElse(7682)
    val PASSWORD = options.get("password").orElse(sys.env.get("LABRADPASSWORD")).getOrElse("")

    val s = ServerConnection(new TestSrv, HOST, PORT, PASSWORD.toCharArray)
    s.connect
    sys.ShutdownHookThread(s.triggerShutdown)
    s.serve
  }
}
