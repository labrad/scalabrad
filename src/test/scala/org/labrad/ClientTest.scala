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

  def testWithClient(name: String)(func: Client => Unit) = test(name) {
    TestUtils.withManager { (host, port, password) =>
      TestUtils.withClient(host = host, port = port, password = password) { c =>
        func(c)
      }
    }
  }

  testWithClient("retrieve list of servers from manager") { c =>
    val data = Await.result(c.send("Manager", "Servers" -> Data.NONE), 10.seconds)(0)
    val servers = data.get[Seq[(Long, String)]]
    assert(servers.contains((1, "Manager")))
    assert(servers.contains((2, "Registry")))
  }

  testWithClient("echo random data from manager") { c =>
    val fs = for (i <- 0 until 1000)
      yield c.send("Manager", "Echo" -> Hydrant.randomData)
    for (f <- fs) Await.result(f, 10.seconds)
  }
}
