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

import Constants._
import data._
import util.{LookupProvider, Util}

class Client(val name: String = "Scala Client",
             val host: String = Util.getEnv("LABRADHOST", DEFAULT_HOST),
             val port: Int = Util.getEnvInt("LABRADPORT", DEFAULT_PORT),
             val password: String = Util.getEnv("LABRADPASSWORD", DEFAULT_PASSWORD)) extends Connection {
  
  protected def loginData = Cluster(Word(PROTOCOL), Str(name))
}

object Client {
  /**
   * Tests some of the basic functionality of the client connection.
   * This method requires that the "Python Test Server" be running
   * to complete all of its tests successfully.
   */
  def main(args: Array[String]) {
    val nEcho = 5
    val nRandomData = 1000
    val nPings = 10000

    val server = "Python Test Server"
    val setting = "Get Random Data"
    
    // connect to LabRAD
    val c = new Client
    c.connect

    // set delay to 1 second
    c.sendAndWait(server) { "Echo Delay" -> Value(1.0, "s") }

    def timeIt(message: String)(func: => Unit) {
      println(message)
      val start = System.currentTimeMillis
      func
      val end = System.currentTimeMillis
      println("done.  elapsed: " + (end - start) + " ms.")
    }
    
    // echo with delays
    timeIt("echo with delays...") {
      val requests = for (i <- 0 until nEcho)
        yield c.send(server) { "Delayed Echo" -> Word(4) }
      for (request <- requests) {
        request()
        println("Got one!")
      }
    }
    
    // random data
    timeIt("getting random data, with printing...") {
      val requests = for (i <- 0 until nRandomData)
        yield c.send(server) { setting -> Data.EMPTY }
      for (request <- requests) {
        val response = request()(0)
        println("got packet: " + response.pretty)
      }
    }

    // random data
    timeIt("getting random data, make pretty, but don't print...") {
      val requests = for (i <- 0 until nRandomData)
        yield c.send(server) { setting -> Data.EMPTY }
      for (request <- requests) request()(0).pretty
    }

    // random data
    timeIt("getting random data, no printing...") {
      val requests = for (i <- 0 until nRandomData)
        yield c.send(server) { setting -> Data.EMPTY }
      for (request <- requests) request()
    }
    
    // debug
    timeIt("getting debug response...") {
      val response = c.sendAndWait(server) { "debug" -> Data.EMPTY } (0)
      println("Debug output: " + response.pretty)
    }
    
    // ping manager
    timeIt("pinging manager " + nPings + " times...") {
      val requests = for (i <- 0 until nPings)
        yield c.send("Manager")()
      for (request <- requests) request()
    }
    
    c.close
  }
}
