package org.labrad.registry

import org.labrad.{RegistryServerProxy, TlsMode}
import org.labrad.annotations._
import org.labrad.concurrent.Futures._
import org.labrad.data._
import org.labrad.manager.ManagerUtils
import org.labrad.registry._
import org.labrad.types._
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.fixture
import scala.concurrent.duration._

class RemoteStoreTest extends fixture.FunSuite with AsyncAssertions {

  import ManagerUtils._

  case class FixtureParam(deadline: Deadline)
  implicit def testDeadline(implicit f: FixtureParam): Deadline = f.deadline

  def withFixture(test: OneArgTest) = {
    withFixture(test.toNoArgTest(FixtureParam(Deadline.now + 1.minute)))
  }

  def withManagers()(func: (ManagerInfo, ManagerInfo, ManagerInfo) => Unit): Unit = {
    withManager() { root =>
      val store1 = new RemoteStore(root.host, root.port, root.credential, tls = TlsMode.OFF)
      withManager(registryStore = Some(store1)) { leaf1 =>
        val store2 = new RemoteStore(root.host, root.port, root.credential, tls = TlsMode.OFF)
        withManager(registryStore = Some(store2)) { leaf2 =>
        }
      }
    }
  }

  test("remote registry gets message when key is created") { implicit f =>
    withManagers() { (root, leaf1, leaf2) =>
      withClient(leaf1) { c1 =>
        withClient(leaf2) { c2 =>

          val r1 = new RegistryServerProxy(c1)
          val r2 = new RegistryServerProxy(c2)

          val w = new Waiter

          val msgId = 1234

          c2.addMessageListener {
            case Message(src, ctx, `msgId`, data) =>
              w { assert(data == Cluster(Str("a"), Bool(false), Bool(true))) }
              w.dismiss
          }

          r1.mkDir("test").await()
          r1.cd("test").await()

          r2.notifyOnChange(msgId, true).await()
          r2.cd("test").await()

          r1.set("a", Str("test")).await()
          w.await(timeout(10.seconds))
        }
      }
    }
  }

  test("remote registry gets message when key is changed") { implicit f =>
    withManagers() { (root, leaf1, leaf2) =>
      withClient(leaf1) { c1 =>
        withClient(leaf2) { c2 =>

          val r1 = new RegistryServerProxy(c1)
          val r2 = new RegistryServerProxy(c2)

          val w = new Waiter

          val msgId = 1234

          c2.addMessageListener {
            case Message(src, ctx, `msgId`, data) =>
              w { assert(data == Cluster(Str("a"), Bool(false), Bool(true))) }
              w.dismiss
          }

          r1.mkDir("test").await()
          r1.cd("test").await()
          r1.set("a", Str("first")).await()

          r2.notifyOnChange(msgId, true).await()
          r2.cd("test").await()

          r1.set("a", Str("second")).await()
          w.await(timeout(10.seconds))
        }
      }
    }
  }

  test("remote registry gets message when key is deleted") { implicit f =>
    withManagers() { (root, leaf1, leaf2) =>
      withClient(leaf1) { c1 =>
        withClient(leaf2) { c2 =>

          val r1 = new RegistryServerProxy(c1)
          val r2 = new RegistryServerProxy(c2)

          val w = new Waiter

          val msgId = 1234

          c2.addMessageListener {
            case Message(src, ctx, `msgId`, data) =>
              w { assert(data == Cluster(Str("a"), Bool(false), Bool(false))) }
              w.dismiss
          }

          r1.mkDir("test").await()
          r1.cd("test").await()
          r1.set("a", Str("first")).await()

          r2.notifyOnChange(msgId, true).await()
          r2.cd("test").await()

          r1.del("a").await()
          w.await(timeout(10.seconds))
        }
      }
    }
  }
}
