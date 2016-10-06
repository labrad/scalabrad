package org.labrad

import org.labrad.concurrent.Futures._
import org.labrad.data._
import org.labrad.manager.ManagerUtils
import org.labrad.util.Await
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.fixture
import scala.concurrent.duration._

class ManagerTest extends fixture.FunSuite with AsyncAssertions {

  import ManagerUtils._

  case class FixtureParam(deadline: Deadline)
  implicit def testDeadline(implicit f: FixtureParam) = f.deadline

  def withFixture(test: OneArgTest) = {
    withFixture(test.toNoArgTest(FixtureParam(Deadline.now + 1.minute)))
  }

  def mgr(c: Client) = new ManagerServerProxy(c)

  test("manager returns lists of servers and settings") { implicit f =>
    withManager() { m =>
      withClient(m) { c =>
        val servers = mgr(c).servers.await()
        assert(servers.size == 2)
        assert(servers.exists { case (1L, "Manager") => true; case _ => false })
        assert(servers.exists { case (_, "Registry") => true; case _ => false })
        TestSrv.withServer(m) { s =>
          val servers2 = mgr(c).servers.await()
          assert(servers2.size == 3)
          assert(servers2.exists { case (_, "Scala Test Server") => true; case _ => false })
        }
      }
    }
  }

  test("manager uses same id when a server reconnects") { implicit f =>
    withManager() { m =>
      withClient(m) { c =>
        val id = TestSrv.withServer(m) { s =>
          mgr(c).lookupServer("Scala Test Server").await()
        }
        Thread.sleep(10000)
        val servers = mgr(c).servers.await()
        assert(!servers.exists { case (_, "Scala Test Server") => true; case _ => false })
        val id2 = TestSrv.withServer(m) { s =>
          mgr(c).lookupServer("Scala Test Server").await()
        }
        assert(id == id2)
      }
    }
  }

  test("manager sends message when server connects and disconnects") { implicit f =>
    withManager() { m =>
      withClient(m) { c =>

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

        mgr(c).subscribeToNamedMessage("Server Connect", connectId, true).await()
        mgr(c).subscribeToNamedMessage("Server Disconnect", disconnectId, true).await()
        TestSrv.withServer(m) { s =>
          connectWaiter.await(timeout(30.seconds))
        }
        disconnectWaiter.await(timeout(30.seconds))

        // test that we can unsubscribe from messages and not get them, then resubscribe and get them again
      }
    }
  }

  test("manager sends named messages when contexts expire") { implicit f =>
    withManager() { m =>
      val expireAllWaiter = new Waiter
      val expireContextWaiter = new Waiter

      val expireAllId = 1234
      val expireContextId = 2345

      withClient(m) { c =>

        mgr(c).subscribeToNamedMessage("Expire All", expireAllId, true).await()
        mgr(c).subscribeToNamedMessage("Expire Context", expireContextId, true).await()
        withClient(m) { c2 =>
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
