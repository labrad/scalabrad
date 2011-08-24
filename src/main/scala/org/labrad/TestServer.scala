/*
 * Copyright 2008 Matthew Neeley
 * 
 * This file is part of JLabrad.
 *
 * JLabrad is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 * 
 * JLabrad is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with JLabrad.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.labrad

import scala.collection._

import annotations._
import data._

/*
 * A very basic skeleton for a server to test the JLabrad API.
 * 
 * Most of the logic for this server is in the TestServerContext class.
 */
@ServerInfo(name = "Scala Test Server",
            doc = "Basic server to test Scalabrad API.")
object TestServer extends Server {
  type ContextType = TestServerContext
  val contextClass = classOf[TestServerContext]
  
  def init { println("init() called on server.") }
  def shutdown { println("shutdown() called on server.") }
}


class TestServerContext(connection: ServerConnection, server: Server, context: Context)
    extends ServerContext(connection, server, context) {
  val registry = mutable.Map.empty[String, Data]

  /** Called when this context is first created */
  def init {
    registry("Test") = Str("blah")
    println("Context %s created.".format(context))
  }


  /** Called when this context has expired.  Do any cleanup here. */
  def expire {
    println("Context %s expired".format(context))
  }


  /**
   * Print out a log message when a setting is called.
   * @param setting
   * @param data
   */
  private def log(setting: String, data: Data) {
    println("%s called [%s]: %s".format(setting, data.tag, data.pretty))
  }


  /**
   * Print out a log message when a setting is called.
   * @param setting
   * @param data
   */
  private def log(setting: String) {
    println("%s called with no data".format(setting))
  }

  
  private def logAny(msg: String) { println(msg) }
  

  /**
   * Make a request to the given server and setting.
   * @param server
   * @param setting
   * @param data
   * @return
   */
  private def makeRequest(server: String, setting: String, data: Data = Data.EMPTY) =
    connection.sendAndWait(server, context)(setting -> data)(0)


  //
  // Settings
  //

  //setting(1, "Echo", """Echoes back any data sent to this setting""") {
  //  case Cluster(Word(w), Str(s)) => 
  //}
  
  /**
   * Echo back the data sent to us.
   * @param data
   * @return
   */
  @Setting(id = 1,
           name = "Echo",
           doc = "Echoes back any data sent to this setting.")
  def echo(data: Data) = {
    log("Echo", data)
    data
  }
  
  
  /**
   * Echo back data after a specified delay.
   * @param data
   * @return
   * @throws InterruptedException
   */
  @Setting(id = 2,
           name = "Delayed Echo",
           doc = "Echoes back data after a specified delay.")
  @Return("? {same as input}")
  def delayedEcho(@Accept("v[s]") delay: Double, payload: Data) = {
    log("Delayed Echo (%g seconds): %s".format(delay, payload.pretty))
    Thread.sleep((delay*1000).toLong)
    payload
  }


  /**
   * Set a key in this context.
   * @param data
   * @return
   */
  @Setting(id = 3,
           name = "Set",
           doc = "Sets a key value pair in the current context.")
  def set(key: String, value: Data) = {
    log("Set: %s = %s".format(key, value))
    registry(key) = value
    value
  }


  /**
   * Get a key from this context.
   * @param data
   * @return
   */
  @Setting(id = 4,
           name = "Get",
           doc = "Gets a key from the current context.")
  def get(key: String) = {
    log("Get: %s".format(key))
    registry.get(key) match {
      case Some(value) => value
      case None => throw new Exception("Invalid key: " + key)
    }
  }


  /**
   * Get all key-value pairs defined in this context.
   * @param data
   * @return
   */
  @Setting(id = 5,
           name = "Get All",
           doc = "Gets all of the key-value pairs defined in this context.")
  def getAll = {
    log("Get All")
    val items = for ((key, value) <- registry)
      yield Cluster(Str(key), value)
    Cluster(items.toSeq: _*)
  }


  /**
   * Get all keys defined in this context.
   * @param data
   * @return
   */
  @Setting(id = 6,
           name = "Keys",
           doc = "Returns a list of all keys defined in this context.")
  @Return("*s")
  def getKeys = {
    log("Keys")
    Arr(for (k <- registry.keys.toSeq.sorted) yield Str(k))
  }


  /**
   * Remove a key from this context
   * @param key
   */
  @Setting(id = 7,
           name = "Remove",
           doc = "Removes the specified key from this context.")
  def remove(key: String) {
    log("Remove: %s".format(key))
    registry -= key
  }


  /**
   * Get a random LabRAD data object.
   * @param data
   * @return
   */
  @Setting(id = 8,
           name = "Get Random Data",
           doc = """Returns random LabRAD data.

                   |If a type is specified, the data will be of that type;
                   |otherwise it will be of a random type.""")
  def getRandomData(typ: String) = {
    log("Get Random Data: %s".format(typ))
    Hydrant.randomData(typ)
  }
  def getRandomData = {
    log("Get Random Data (no type)")
    Hydrant.randomData
  }


  /**
   * Get Random data by calling the python test server
   * @param data
   * @return
   * @throws InterruptedException
   * @throws ExecutionException
   */
  @Setting(id = 9,
           name = "Get Random Data Remote",
           doc = "Fetches random data by making a request to the python test server.")
  def getRandomDataRemote(typ: Option[String] = None) =
    typ match {
      case Some(typ) =>
        log("Get Random Data Remote: %s".format(typ))
        makeRequest("Python Test Server", "Get Random Data", Str(typ))
        
      case None =>
        log("Get Random Data Remote (no type)")
        makeRequest("Python Test Server", "Get Random Data")
    }
  //def getRandomDataRemote = {
  //  log("Get Random Data Remote (no type)")
  //  makeRequest("Python Test Server", "Get Random Data")
  //}


  /**
   * Forward a request on to another server.
   * @param data
   * @return
   * @throws InterruptedException
   * @throws ExecutionException
   */
  @Setting(id = 10,
           name = "Forward Request",
           doc = "Forwards a request on to another server, specified by name and setting.")
  def forwardRequest(server: String, setting: String, payload: Data) = {
    log("Forward Request: server='%s', setting='%s', payload=%s".format(server, setting, payload))
    makeRequest(server, setting, payload)
  }

  // commented annotations will give errors

  @Setting(id = 11,
           name = "Test No Args",
           doc = "Test setting that takes no arguments.")
  def noArgs = {
    log("Test No Args")
    true
  }

  @Setting(id = 12,
           name = "Test No Return",
           doc = "Test setting with no return value.")
  def noReturn(data: Data) {
    log("Test No Return", data)
  }

  @Setting(id = 13,
           name = "Test No Args No Return",
           doc = "Test setting that takes no arguments and has no return value.")
  def noArgsNoReturn {
    log("Test No Args No Return")
  }
}
