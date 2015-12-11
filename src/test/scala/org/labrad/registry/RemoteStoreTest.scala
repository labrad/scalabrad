package org.labrad.registry

import org.labrad.{ManagerInfo, RegistryServerProxy, TlsMode}
import org.labrad.annotations._
import org.labrad.data._
import org.labrad.registry._
import org.labrad.types._
import org.scalatest.{FunSuite, Matchers, Tag}
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.time.SpanSugar._
import scala.collection._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class RemoteStoreTest extends FunSuite with Matchers with AsyncAssertions {

  import org.labrad.TestUtils._

  def withManagers()(func: (ManagerInfo, ManagerInfo, ManagerInfo) => Unit): Unit = {
    withManager() { root =>
      val store1 = new RemoteStore(root.host, root.port, root.password, tls = TlsMode.OFF)
      withManager(registryStore = Some(store1)) { leaf1 =>
        val store2 = new RemoteStore(root.host, root.port, root.password, tls = TlsMode.OFF)
        withManager(registryStore = Some(store2)) { leaf2 =>
        }
      }
    }
  }

  test("remote registry gets message when key is created") {
    withManagers() { (root, leaf1, leaf2) =>
      withClient(leaf1.host, leaf1.port, leaf1.password) { c1 =>
        withClient(leaf2.host, leaf2.port, leaf2.password) { c2 =>

          val r1 = new RegistryServerProxy(c1)
          val r2 = new RegistryServerProxy(c2)

          val w = new Waiter

          val msgId = 1234

          c2.addMessageListener {
            case Message(src, ctx, `msgId`, data) =>
              w { assert(data == Cluster(Str("a"), Bool(false), Bool(true))) }
              w.dismiss
          }

          await(r1.mkDir("test"))
          await(r1.cd("test"))

          await(r2.notifyOnChange(msgId, true))
          await(r2.cd("test"))

          await(r1.set("a", Str("test")))
          w.await(timeout(10.seconds))
        }
      }
    }
  }

  test("remote registry gets message when key is changed") {
    withManagers() { (root, leaf1, leaf2) =>
      withClient(leaf1.host, leaf1.port, leaf1.password) { c1 =>
        withClient(leaf2.host, leaf2.port, leaf2.password) { c2 =>

          val r1 = new RegistryServerProxy(c1)
          val r2 = new RegistryServerProxy(c2)

          val w = new Waiter

          val msgId = 1234

          c2.addMessageListener {
            case Message(src, ctx, `msgId`, data) =>
              w { assert(data == Cluster(Str("a"), Bool(false), Bool(true))) }
              w.dismiss
          }

          await(r1.mkDir("test"))
          await(r1.cd("test"))
          await(r1.set("a", Str("first")))

          await(r2.notifyOnChange(msgId, true))
          await(r2.cd("test"))

          await(r1.set("a", Str("second")))
          w.await(timeout(10.seconds))
        }
      }
    }
  }

  test("remote registry gets message when key is deleted") {
    withManagers() { (root, leaf1, leaf2) =>
      withClient(leaf1.host, leaf1.port, leaf1.password) { c1 =>
        withClient(leaf2.host, leaf2.port, leaf2.password) { c2 =>

          val r1 = new RegistryServerProxy(c1)
          val r2 = new RegistryServerProxy(c2)

          val w = new Waiter

          val msgId = 1234

          c2.addMessageListener {
            case Message(src, ctx, `msgId`, data) =>
              w { assert(data == Cluster(Str("a"), Bool(false), Bool(false))) }
              w.dismiss
          }

          await(r1.mkDir("test"))
          await(r1.cd("test"))
          await(r1.set("a", Str("first")))

          await(r2.notifyOnChange(msgId, true))
          await(r2.cd("test"))

          await(r1.del("a"))
          w.await(timeout(10.seconds))
        }
      }
    }
  }
}
