package org.labrad

import java.io.File
import java.nio.ByteOrder
import java.util.{Date, Random}
import org.labrad.data._
import org.labrad.manager.CentralNode
import org.labrad.types._
import org.labrad.util.{Logging, Util}
import org.scalatest.FunSuite
import scala.concurrent.Await
import scala.concurrent.duration._

class ClientTests extends FunSuite /*with Logging*/ {

  def withManager[T](f: (String, Int, String) => T): T = {
    val host = "localhost"
    val port = scala.util.Random.nextInt(50000) + 10000 //Util.findAvailablePort()
    val remotePort = scala.util.Random.nextInt(50000) + 10000 //Util.findAvailablePort()
    val password = "testPassword12345!@#$%"

    val registryRoot = File.createTempFile("labrad-registry", "")
    registryRoot.delete()
    registryRoot.mkdir()

    val manager = new CentralNode(port, password, registryRoot, remotePort)
    Thread.sleep(5000)
    try {
      f(host, port, password)
    } finally {
      manager.stop()
    }
  }

  def testWithClient(name: String)(func: Client => Unit) = test(name) {
    withManager { (host, port, password) =>
      val c = new Client(host = host, port = port, password = password)
      c.connect
      try {
        func(c)
      } finally {
        c.close
      }
    }
  }

  testWithClient("retrieve list of servers from manager") { c =>
    val data = Await.result(c.send("Manager", "Servers" -> Data.NONE), 10.seconds)(0)
    val servers = data.getDataSeq map { case Cluster(UInt(id), Str(name)) => (id, name) }
    assert(servers.contains((1, "Manager")))
    assert(servers.contains((2, "Registry")))
  }

  testWithClient("echo random data from manager") { c =>
    val fs = for (i <- 0 until 1000)
      yield c.send("Manager", "Echo" -> Hydrant.randomData)
    for (f <- fs) Await.result(f, 10.seconds)
  }
}
