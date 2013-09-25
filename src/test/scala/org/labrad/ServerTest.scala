package org.labrad

import org.labrad.annotations._
import org.labrad.data._
import org.scalatest.FunSuite
import org.scalatest.concurrent.AsyncAssertions
import scala.collection._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.reflect.runtime.universe

@IsServer(name = "Scala Test Server",
           doc = "Basic server to test Scalabrad API.")
object TestSrv extends Server[TestCtx] {
  def init: Unit = println("init() called on server.")
  def shutdown: Unit = println("shutdown() called on server.")
}


class TestCtx(cxn: ServerConnection[TestCtx], server: Server[TestCtx], context: Context)
extends ServerContext(cxn, server, context) {
  private val registry = mutable.Map.empty[String, Data]

  def init: Unit = {
    registry("Test") = Str("blah")
    println("Context %s created.".format(context))
  }

  def expire: Unit = {
    println("Context %s expired".format(context))
  }

  private def log(setting: String, data: Data): Unit = println(s"$setting called [${data.t}]: $data")
  private def log(setting: String): Unit = println(s"$setting called")
  private def logAny(msg: String): Unit = println(msg)

  private def makeRequest(server: String, setting: String, data: Data = Data.NONE) =
    Await.result(cxn.send(server, context, setting -> data), 10.seconds)(0)


  //
  // Settings
  //

  @Setting(id = 1,
           name = "Echo",
           doc = "Echoes back any data sent to this setting.")
  def echo(r: RequestContext, data: Data): Data = {
    log("Echo", data)
    data
  }

  @Setting(id = 2,
           name = "Delayed Echo",
           doc = "Echoes back data after a specified delay.")
  def delayedEcho(r: RequestContext, @Accept("v[s]") delay: Double, payload: Data): Data = {
    log("Delayed Echo (%g seconds): %s".format(delay, payload))
    Thread.sleep((delay*1000).toLong)
    payload
  }

  @Setting(id = 3,
           name = "Set",
           doc = "Sets a key value pair in the current context.")
  def set(r: RequestContext, key: String, value: Data): Data = {
    log("Set: %s = %s".format(key, value))
    registry(key) = value
    value
  }

  @Setting(id = 4,
           name = "Get",
           doc = "Gets a key from the current context.")
  def get(r: RequestContext, key: String): Data = {
    log("Get: %s".format(key))
    registry.get(key) match {
      case Some(value) => value
      case None => sys.error("Invalid key: " + key)
    }
  }

  @Setting(id = 5,
           name = "Get All",
           doc = "Gets all of the key-value pairs defined in this context.")
  def getAll(r: RequestContext): Seq[(String, Data)] = {
    log("Get All")
    registry.toSeq.sortBy(_._1)
  }

  @Setting(id = 6,
           name = "Keys",
           doc = "Returns a list of all keys defined in this context.")
  def getKeys(r: RequestContext): Seq[String] = {
    log("Keys")
    registry.keys.toSeq.sorted
  }

  @Setting(id = 7,
           name = "Remove",
           doc = "Removes the specified key from this context.")
  def remove(r: RequestContext, key: String): Unit = {
    log("Remove: %s".format(key))
    registry -= key
  }

  @Setting(id = 8,
           name = "Get Random Data",
           doc = """Returns random LabRAD data.

                   |If a type is specified, the data will be of that type;
                   |otherwise it will be of a random type.""")
  def getRandomData(r: RequestContext, typ: String): Data = {
    log("Get Random Data: %s".format(typ))
    Hydrant.randomData(typ)
  }
  def getRandomData(r: RequestContext): Data = {
    log("Get Random Data (no type)")
    Hydrant.randomData
  }

  @Setting(id = 9,
           name = "Get Random Data Remote",
           doc = "Fetches random data by making a request to the python test server.")
  def getRandomDataRemote(r: RequestContext, typ: Option[String] = None): Data = typ match {
    case Some(typ) =>
      log("Get Random Data Remote: %s".format(typ))
      makeRequest("Python Test Server", "Get Random Data", Str(typ))

    case None =>
      log("Get Random Data Remote (no type)")
      makeRequest("Python Test Server", "Get Random Data")
  }

  @Setting(id = 10,
           name = "Forward Request",
           doc = "Forwards a request on to another server, specified by name and setting.")
  def forwardRequest(r: RequestContext, server: String, setting: String, payload: Data): Data = {
    log("Forward Request: server='%s', setting='%s', payload=%s".format(server, setting, payload))
    makeRequest(server, setting, payload)
  }

  @Setting(id = 11,
           name = "Test No Args",
           doc = "Test setting that takes no arguments.")
  def noArgs(r: RequestContext): Boolean = {
    log("Test No Args")
    true
  }

  @Setting(id = 12,
           name = "Test No Return",
           doc = "Test setting with no return value.")
  def noReturn(r: RequestContext, data: Data): Unit = {
    log("Test No Return", data)
  }

  @Setting(id = 13,
           name = "Test No Args No Return",
           doc = "Test setting that takes no arguments and has no return value.")
  def noArgsNoReturn(r: RequestContext): Unit = {
    log("Test No Args No Return")
  }
}

class ServerTest extends FunSuite with AsyncAssertions {

  import TestUtils._

  test("server can log in and log out of manager") {
    withManager { (host, port, password) =>
      withServer(host, port, password) {
        withClient(host, port, password) { c =>
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
      }
    }
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

    val s = ServerConnection(TestSrv, HOST, PORT, PASSWORD)
    s.connect
    sys.ShutdownHookThread(s.triggerShutdown)
    s.serve
  }
}
