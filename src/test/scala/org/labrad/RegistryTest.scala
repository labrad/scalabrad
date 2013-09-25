package org.labrad

import org.labrad.annotations._
import org.labrad.data._
import org.scalatest.FunSuite
import org.scalatest.concurrent.AsyncAssertions
import scala.collection._
import scala.concurrent.{Await, Future}
import org.scalatest.time.SpanSugar._

class RegistryTest extends FunSuite with AsyncAssertions {

  import TestUtils._

  test("registry can store and retrieve arbitrary data") {
    withManager { (host, port, password) =>
      withClient(host, port, password) { c =>
        await(c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")))
        try {
          for (i <- 0 until 1000) {
            val tpe = Hydrant.randomType
            val data = Hydrant.randomData(tpe)
            await(c.send("Registry", "set" -> Cluster(Str("a"), data)))
            val resp = await(c.send("Registry", "get" -> Str("a")))(0)
            await(c.send("Registry", "del" -> Str("a")))
            assert(resp ~== data, s"${resp} (type=${resp.t}) is not equal to ${data} (type=${data.t})")
          }
        } finally {
          await(c.send("Registry", "cd" -> Str(".."), "rmdir" -> Str("test")))
        }
      }
    }
  }

  test("registry sends message when key is created") {
    withManager { (host, port, password) =>
      withClient(host, port, password) { c =>

        val w = new Waiter

        val msgId = 1234

        c.addMessageListener {
          case Message(src, ctx, `msgId`, data) =>
            w { assert(data == Cluster(Str("a"), Bool(false), Bool(true))) }
            w.dismiss
        }

        await(c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")))
        await(c.send("Registry", "Notify On Change" -> Cluster(UInt(msgId), Bool(true))))
        await(c.send("Registry", "set" -> Cluster(Str("a"), Str("test"))))
        w.await(timeout(10.seconds))
      }
    }
  }

  test("registry sends message when key is changed") {
    withManager { (host, port, password) =>
      withClient(host, port, password) { c =>

        val w = new Waiter

        val msgId = 1234

        c.addMessageListener {
          case Message(src, ctx, `msgId`, data) =>
            w { assert(data == Cluster(Str("a"), Bool(false), Bool(true))) }
            w.dismiss
        }

        await(c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")))
        await(c.send("Registry", "set" -> Cluster(Str("a"), Str("first"))))
        await(c.send("Registry", "Notify On Change" -> Cluster(UInt(msgId), Bool(true))))
        await(c.send("Registry", "set" -> Cluster(Str("a"), Str("second"))))
        w.await
      }
    }
  }

  test("registry sends message when key is deleted") {
    withManager { (host, port, password) =>
      withClient(host, port, password) { c =>

        val w = new Waiter

        val msgId = 1234

        c.addMessageListener {
          case Message(src, ctx, `msgId`, data) =>
            w { assert(data == Cluster(Str("a"), Bool(false), Bool(false))) }
            w.dismiss
        }

        await(c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")))
        await(c.send("Registry", "set" -> Cluster(Str("a"), Str("first"))))
        await(c.send("Registry", "Notify On Change" -> Cluster(UInt(msgId), Bool(true))))
        await(c.send("Registry", "del" -> Str("a")))
        w.await
      }
    }
  }
}
