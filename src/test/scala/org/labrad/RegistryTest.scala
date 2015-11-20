package org.labrad

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

class RegistryTest extends FunSuite with Matchers with AsyncAssertions {

  import TestUtils._

  def testAllBackends(testName: String)(func: (RegistryStore, Boolean) => Unit): Unit = {
    test(s"BinaryFileStore: $testName") {
      withTempDir { dir => func(new BinaryFileStore(dir), true) }
    }

    test(s"DelphiFileStore: $testName", Tag("delphi")) {
      withTempDir { dir => func(new DelphiFileStore(dir), false) }
    }

    test(s"SQLiteStore: $testName") {
      withTempFile { file => func(SQLiteStore(file), true) }
    }
  }

  testAllBackends("registry can store and retrieve arbitrary data") { (backend, exact) =>
    val (loc, _) = backend.child(backend.root, "test", create = true)
    for (i <- 0 until 1000) {
      val tpe = Hydrant.randomType
      val data = Hydrant.randomData(tpe)
      backend.setValue(loc, "a", data)
      val resp = backend.getValue(loc, "a", default = None)
      backend.delete(loc, "a")

      // Inexact matching function used for semi-lossy registry backends (e.g. delphi).
      // Uses approximate equality for floats, and also allows for the loss of
      // element type information in empty lists.
      def matches(in: Data, out: Data): Boolean = {
        (in.t, out.t) match {
          // if array is empty, it may come out as *_ instead of *x
          case (TArr(e1, d1), TArr(TNone, d2)) if d1 == d2 =>
            in.arrayShape.product == 0

          case (TArr(e1, d1), TArr(e2, d2)) if d1 == d2 =>
            (in.arrayShape.toSeq == out.arrayShape.toSeq) &&
            (in.flatIterator zip out.flatIterator).forall { case (e1, e2) =>
              matches(e1, e2)
            }

          case (TCluster(elems1 @ _*), TCluster(elems2 @ _*)) if elems1.size == elems2.size =>
            (in.clusterIterator zip out.clusterIterator).forall { case (e1, e2) =>
              matches(e1, e2)
            }

          case _ =>
            in ~== out
        }
      }

      if (exact) {
        assert(resp == data, s"output ${resp} (type=${resp.t}) does not equal input ${data} (type=${data.t})")
      } else {
        assert(matches(in=data, out=resp), s"output ${resp} (type=${resp.t}) does not match input ${data} (type=${data.t})")
      }
    }
  }

  testAllBackends("registry can deal with unicode and strange characters in directory names") { (backend, exact) =>
    val dir = "<\u03C0|\u03C1>??+*\\/:|"
    backend.child(backend.root, dir, create = true)
    val (dirs, _) = backend.dir(backend.root)
    assert(dirs contains dir)
  }

  testAllBackends("registry can deal with unicode and strange characters in key names") { (backend, exact) =>
    val key = "<\u03C0|\u03C1>??+*\\/:|"
    val data = Str("Hello!")
    backend.setValue(backend.root, key, data)
    val (_, keys) = backend.dir(backend.root)
    assert(keys contains key)
    val result = backend.getValue(backend.root, key, default = None)
    assert(result == data)
  }

  test("registry can store and retrieve arbitrary data") {
    withManager() { m =>
      withClient(m.host, m.port, m.password) { c =>
        await(c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")))
        try {
          for (i <- 0 until 1000) {
            val tpe = Hydrant.randomType
            val data = Hydrant.randomData(tpe)
            await(c.send("Registry", "set" -> Cluster(Str("a"), data)))
            val resp = await(c.send("Registry", "get" -> Str("a")))(0)
            await(c.send("Registry", "del" -> Str("a")))
            assert(resp == data, s"${resp} (type=${resp.t}) is not equal to ${data} (type=${data.t})")
          }
        } finally {
          await(c.send("Registry", "cd" -> Str(".."), "rmdir" -> Str("test")))
        }
      }
    }
  }

  test("registry can deal with unicode and strange characters in directory names", Tag("chars")) {
    withManager() { m =>
      withClient(m.host, m.port, m.password) { c =>
        val dir = "<\u03C0|\u03C1>??+*\\/:|"
        await(c.send("Registry", "mkdir" -> Str(dir)))
        val (dirs, _) = await(c.send("Registry", "dir" -> Data.NONE))(0).get[(Seq[String], Seq[String])]
        assert(dirs contains dir)
        await(c.send("Registry", "cd" -> Str(dir)))
      }
    }
  }

  test("registry can deal with unicode and strange characters in key names", Tag("chars")) {
    withManager() { m =>
      withClient(m.host, m.port, m.password) { c =>
        val key = "<\u03C0|\u03C1>??+*\\/:|"
        val data = Str("Hello!")
        await(c.send("Registry", "set" -> Cluster(Str(key), Str("Hello!"))))
        val (_, keys) = await(c.send("Registry", "dir" -> Data.NONE))(0).get[(Seq[String], Seq[String])]
        assert(keys contains key)
        val result = await(c.send("Registry", "get" -> Str(key)))(0)
        assert(result == data)
      }
    }
  }

  test("registry cd with no arguments stays in same directory") {
    withManager() { m =>
      withClient(m.host, m.port, m.password) { c =>
        await(c.send("Registry", "cd" -> Cluster(Arr(Str("test"), Str("a")), Bool(true))))
        val result = await(c.send("Registry", "cd" -> Data.NONE))(0)
        assert(result.get[Seq[String]] == Seq("", "test", "a"))
      }
    }
  }

  test("registry sends message when key is created") {
    withManager() { m =>
      withClient(m.host, m.port, m.password) { c =>

        val w = new Waiter

        val msgId = 1234

        c.addMessageListener {
          case Message(src, ctx, `msgId`, data) =>
            w { assert(data == Cluster(Str("a"), Bool(false), Bool(true))) }
            w.dismiss
        }

        await(c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")))
        await(c.send("Registry", "Notify on Change" -> Cluster(UInt(msgId), Bool(true))))
        await(c.send("Registry", "set" -> Cluster(Str("a"), Str("test"))))
        w.await(timeout(10.seconds))
      }
    }
  }

  test("registry sends message when key is changed") {
    withManager() { m =>
      withClient(m.host, m.port, m.password) { c =>

        val w = new Waiter

        val msgId = 1234

        c.addMessageListener {
          case Message(src, ctx, `msgId`, data) =>
            w { assert(data == Cluster(Str("a"), Bool(false), Bool(true))) }
            w.dismiss
        }

        await(c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")))
        await(c.send("Registry", "set" -> Cluster(Str("a"), Str("first"))))
        await(c.send("Registry", "Notify on Change" -> Cluster(UInt(msgId), Bool(true))))
        await(c.send("Registry", "set" -> Cluster(Str("a"), Str("second"))))
        w.await
      }
    }
  }

  test("registry sends message when key is deleted") {
    withManager() { m =>
      withClient(m.host, m.port, m.password) { c =>

        val w = new Waiter

        val msgId = 1234

        c.addMessageListener {
          case Message(src, ctx, `msgId`, data) =>
            w { assert(data == Cluster(Str("a"), Bool(false), Bool(false))) }
            w.dismiss
        }

        await(c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")))
        await(c.send("Registry", "set" -> Cluster(Str("a"), Str("first"))))
        await(c.send("Registry", "Notify on Change" -> Cluster(UInt(msgId), Bool(true))))
        await(c.send("Registry", "del" -> Str("a")))
        w.await
      }
    }
  }

  test("deleting a registry directory containing a dir should fail") {
    withManager() { m =>
      withClient(m.host, m.port, m.password) { c =>

        val reg = new RegistryServerProxy(c)
        await(reg.cd(Seq("a", "b"), true))

        def dirs(): Seq[String] = {
          val (ds, ks) = await(reg.dir())
          ds
        }

        await(reg.cd(".."))
        assert(dirs() contains "b")

        await(reg.cd(".."))
        assert(dirs() contains "a")

        an[Exception] should be thrownBy {
          await(reg.rmDir("a"))
        }
      }
    }
  }

  test("deleting a registry directory containing a key should fail") {
    withManager() { m =>
      withClient(m.host, m.port, m.password) { c =>

        val reg = new RegistryServerProxy(c)
        await(reg.cd(Seq("a"), true))
        await(reg.set("blah", Str("test")))

        val (_, keys) = await(reg.dir())
        assert(keys contains "blah")

        await(reg.cd(".."))

        an[Exception] should be thrownBy {
          await(reg.rmDir("a"))
        }
      }
    }
  }

  test("creating a registry directory with empty name should fail", Tag("empties")) {
    withManager() { m =>
      withClient(m.host, m.port, m.password) { c =>

        val reg = new RegistryServerProxy(c)
        await(reg.cd(Seq("test"), true))

        an[Exception] should be thrownBy {
          await(reg.mkDir(""))
        }
      }
    }
  }

  test("creating a registry key with empty name should fail", Tag("empties")) {
    withManager() { m =>
      withClient(m.host, m.port, m.password) { c =>

        val reg = new RegistryServerProxy(c)
        await(reg.cd(Seq("test"), true))

        an[Exception] should be thrownBy {
          await(reg.set("", Str("not allowed")))
        }
      }
    }
  }
}

object RegistryTest {

  import TestUtils._

  def main(args: Array[String]): Unit = {
    val (host, port) = args match {
      case Array(host) =>
        host.split(":") match {
          case Array(host, port) => host -> port.toInt
          case Array(host) => host -> 7682
        }

      case Array() =>
        "localhost" -> 7682
    }

    withClient(host, port, password = Array()) { c =>
      val reg = new RegistryServerProxy(c)
      val (dirs, keys) = await(reg.dir())
      if (!dirs.contains("test")) {
        await(reg.mkDir("test"))
      }
      await(reg.cd("test"))

      def mkdir(level: Int = 0): Unit = {
        if (level < 3) {
          for (i <- 0 until 3) {
            val dir = s"dir$i"
            await(reg.mkDir(dir))
            await(reg.cd(dir))
            mkdir(level + 1)
            await(reg.cd(".."))
          }
        }
        for (i <- 0 until 10) {
          val key = f"key$i%03d"
          val tpe = Hydrant.randomType
          val data = Hydrant.randomData(tpe)
          await(reg.set(key, data))
          val resp = await(reg.get(key))
          assert(resp ~== data, s"${resp} (type=${resp.t}) is not equal to ${data} (type=${data.t})")
        }
      }
      mkdir()
    }
  }
}
