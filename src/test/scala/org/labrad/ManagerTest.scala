package org.labrad

import org.labrad.data._
import org.scalatest.{FunSuite, Tag}
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.time.SpanSugar._
import scala.concurrent.ExecutionContext.Implicits.global

class ManagerTest extends FunSuite with AsyncAssertions {

  import TestUtils._

  def mgr(c: Client) = new ManagerServerProxy(c)

  test("manager returns lists of servers and settings") {
    withManager { (host, port, password) =>
      withClient(host, port, password) { c =>
        val servers = await(mgr(c).servers)
        assert(servers.size == 2)
        assert(servers.exists { case (1L, "Manager") => true; case _ => false })
        assert(servers.exists { case (_, "Registry") => true; case _ => false })
        withServer(host, port, password) { s =>
          val servers2 = await(mgr(c).servers)
          assert(servers2.size == 3)
          assert(servers2.exists { case (_, "Scala Test Server") => true; case _ => false })
        }
      }
    }
  }

  test("manager uses same id when a server reconnects") {
    withManager { (host, port, password) =>
      withClient(host, port, password) { c =>
        val id = withServer(host, port, password) { s =>
          await(mgr(c).lookupServer("Scala Test Server"))
        }
        Thread.sleep(10000)
        val servers = await(mgr(c).servers)
        assert(!servers.exists { case (_, "Scala Test Server") => true; case _ => false })
        val id2 = withServer(host, port, password) { s =>
          await(mgr(c).lookupServer("Scala Test Server"))
        }
        assert(id == id2)
      }
    }
  }

  test("manager sends message when server connects and disconnects") {
    withManager { (host, port, password) =>
      withClient(host, port, password) { c =>

        val connectWaiter = new Waiter
        val disconnectWaiter = new Waiter

        val connectId = 1234
        val disconnectId = 2345

        c.addMessageListener {
          case Message(src, ctx, `connectId`, Cluster(UInt(id), Str(name))) =>
            connectWaiter { assert(name == "Scala Test Server") }
            connectWaiter.dismiss
        }

        c.addMessageListener {
          case Message(src, ctx, `disconnectId`, Cluster(UInt(id), Str(name))) =>
            disconnectWaiter { assert(name == "Scala Test Server") }
            disconnectWaiter.dismiss
        }

        await(mgr(c).subscribeToNamedMessage("Server Connect", connectId, true))
        await(mgr(c).subscribeToNamedMessage("Server Disconnect", disconnectId, true))
        withServer(host, port, password) { s =>
          connectWaiter.await(timeout(30.seconds))
        }
        disconnectWaiter.await(timeout(30.seconds))

        // test that we can unsubscribe from messages and not get them, then resubscribe and get them again
      }
    }
  }

  test("manager sends named messages when contexts expire") {
    withManager { (host, port, password) =>
      val expireAllWaiter = new Waiter
      val expireContextWaiter = new Waiter

      val expireAllId = 1234
      val expireContextId = 2345

      withClient(host, port, password) { c =>

        await(mgr(c).subscribeToNamedMessage("Expire All", expireAllId, true))
        await(mgr(c).subscribeToNamedMessage("Expire Context", expireContextId, true))
        withClient(host, port, password) { c2 =>
          // after requesting expiration, should get an expire context message
          val context = Context(c2.id, 20)
          c.addMessageListener {
            case message @ Message(src, ctx, `expireContextId`, Cluster(UInt(high), UInt(low))) =>
              expireContextWaiter {
                assert(src == c.id)
                assert(high == c2.id)
                assert(low == context.low)
              }
              expireContextWaiter.dismiss
          }
          c.send("Manager", context, "Expire Context" -> Data.NONE)
          expireContextWaiter.await(timeout(30.seconds))

          // after disconnecting, should get an expire all message
          c.addMessageListener {
            case Message(src, ctx, `expireAllId`, UInt(id)) =>
              expireAllWaiter { assert(id == c2.id) }
              expireAllWaiter.dismiss
          }
        }
        expireAllWaiter.await(timeout(30.seconds))
      }
    }
  }
}
