package org.labrad

import org.labrad.data._
import org.labrad.manager.ManagerUtils
import org.scalatest.FunSuite
import scala.concurrent.Await
import scala.concurrent.duration._

class ClientTest extends FunSuite {

  def testWithClient(name: String)(func: Client => Unit): Unit = test(name) {
    ManagerUtils.withManager() { m =>
      ManagerUtils.withClient(m) { c =>
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
