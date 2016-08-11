package org.labrad

import org.labrad.annotations._
import org.labrad.data._
import org.labrad.manager.ManagerUtils
import org.labrad.registry._
import org.labrad.types._
import org.labrad.util.{Await, Files}
import org.scalatest.{FunSuite, Matchers, Tag}
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.time.SpanSugar._
import scala.collection._

class RegistryTest extends FunSuite with Matchers with AsyncAssertions {

  import ManagerUtils._

  def testAllBackends(testName: String)(func: (RegistryStore, Boolean) => Unit): Unit = {
    test(s"BinaryFileStore: $testName") {
      Files.withTempDir { dir => func(new BinaryFileStore(dir), true) }
    }

    test(s"DelphiFileStore: $testName", Tag("delphi")) {
      Files.withTempDir { dir => func(new DelphiFileStore(dir), false) }
    }

    test(s"SQLiteStore: $testName") {
      Files.withTempFile { file => func(SQLiteStore(file), true) }
    }
  }

  testAllBackends("registry can store and retrieve arbitrary data") { (backend, exact) =>
    val loc = backend.child(backend.root, "test", create = true)
    for (i <- 0 until 100) {
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
      withClient(m) { c =>
        Await(c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")))
        try {
          for (i <- 0 until 1000) {
            val tpe = Hydrant.randomType
            val data = Hydrant.randomData(tpe)
            Await(c.send("Registry", "set" -> Cluster(Str("a"), data)))
            val resp = Await(c.send("Registry", "get" -> Str("a")))(0)
            Await(c.send("Registry", "del" -> Str("a")))
            assert(resp == data, s"${resp} (type=${resp.t}) is not equal to ${data} (type=${data.t})")
          }
        } finally {
          Await(c.send("Registry", "cd" -> Str(".."), "rmdir" -> Str("test")))
        }
      }
    }
  }

  test("registry can deal with unicode and strange characters in directory names", Tag("chars")) {
    withManager() { m =>
      withClient(m) { c =>
        val dir = "<\u03C0|\u03C1>??+*\\/:|"
        Await(c.send("Registry", "mkdir" -> Str(dir)))
        val (dirs, _) = Await(c.send("Registry", "dir" -> Data.NONE))(0).get[(Seq[String], Seq[String])]
        assert(dirs contains dir)
        Await(c.send("Registry", "cd" -> Str(dir)))
      }
    }
  }

  test("registry can deal with unicode and strange characters in key names", Tag("chars")) {
    withManager() { m =>
      withClient(m) { c =>
        val key = "<\u03C0|\u03C1>??+*\\/:|"
        val data = Str("Hello!")
        Await(c.send("Registry", "set" -> Cluster(Str(key), Str("Hello!"))))
        val (_, keys) = Await(c.send("Registry", "dir" -> Data.NONE))(0).get[(Seq[String], Seq[String])]
        assert(keys contains key)
        val result = Await(c.send("Registry", "get" -> Str(key)))(0)
        assert(result == data)
      }
    }
  }

  test("registry cd with no arguments stays in same directory") {
    withManager() { m =>
      withClient(m) { c =>
        Await(c.send("Registry", "cd" -> Cluster(Arr(Str("test"), Str("a")), Bool(true))))
        val result = Await(c.send("Registry", "cd" -> Data.NONE))(0)
        assert(result.get[Seq[String]] == Seq("", "test", "a"))
      }
    }
  }

  test("registry sends message when key is created") {
    withManager() { m =>
      withClient(m) { c =>

        val w = new Waiter

        val msgId = 1234

        c.addMessageListener {
          case Message(src, ctx, `msgId`, data) =>
            w { assert(data == Cluster(Str("a"), Bool(false), Bool(true))) }
            w.dismiss
        }

        Await(c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")))
        Await(c.send("Registry", "Notify on Change" -> Cluster(UInt(msgId), Bool(true))))
        Await(c.send("Registry", "set" -> Cluster(Str("a"), Str("test"))))
        w.await(timeout(10.seconds))
      }
    }
  }

  test("registry sends message when key is changed") {
    withManager() { m =>
      withClient(m) { c =>

        val w = new Waiter

        val msgId = 1234

        c.addMessageListener {
          case Message(src, ctx, `msgId`, data) =>
            w { assert(data == Cluster(Str("a"), Bool(false), Bool(true))) }
            w.dismiss
        }

        Await(c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")))
        Await(c.send("Registry", "set" -> Cluster(Str("a"), Str("first"))))
        Await(c.send("Registry", "Notify on Change" -> Cluster(UInt(msgId), Bool(true))))
        Await(c.send("Registry", "set" -> Cluster(Str("a"), Str("second"))))
        w.await
      }
    }
  }

  test("registry sends message when key is deleted") {
    withManager() { m =>
      withClient(m) { c =>

        val w = new Waiter

        val msgId = 1234

        c.addMessageListener {
          case Message(src, ctx, `msgId`, data) =>
            w { assert(data == Cluster(Str("a"), Bool(false), Bool(false))) }
            w.dismiss
        }

        Await(c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")))
        Await(c.send("Registry", "set" -> Cluster(Str("a"), Str("first"))))
        Await(c.send("Registry", "Notify on Change" -> Cluster(UInt(msgId), Bool(true))))
        Await(c.send("Registry", "del" -> Str("a")))
        w.await
      }
    }
  }

  testAllBackends("deleting a registry directory containing a dir should fail") { (backend, exact) =>
    val root = backend.root
    val a = backend.child(root, "a", create = true)
    val b = backend.child(a, "b", create = true)

    assert(backend.dir(root)._1 contains "a")
    assert(backend.dir(a)._1 contains "b")

    intercept[Exception] {
      backend.rmDir(root, "a")
    }
  }

  test("deleting a registry directory containing a dir should fail") {
    withManager() { m =>
      withClient(m) { c =>

        val reg = new RegistryServerProxy(c)
        Await(reg.cd(Seq("a", "b"), true))

        def dirs(): Seq[String] = {
          val (ds, ks) = Await(reg.dir())
          ds
        }

        Await(reg.cd(".."))
        assert(dirs() contains "b")

        Await(reg.cd(".."))
        assert(dirs() contains "a")

        intercept[Exception] {
          Await(reg.rmDir("a"))
        }
      }
    }
  }

  test("deleting a registry directory containing a key should fail") {
    withManager() { m =>
      withClient(m) { c =>

        val reg = new RegistryServerProxy(c)
        Await(reg.cd(Seq("a"), true))
        Await(reg.set("blah", Str("test")))

        val (_, keys) = Await(reg.dir())
        assert(keys contains "blah")

        Await(reg.cd(".."))

        intercept[Exception] {
          Await(reg.rmDir("a"))
        }
      }
    }
  }

  test("creating a registry directory with empty name should fail", Tag("empties")) {
    withManager() { m =>
      withClient(m) { c =>

        val reg = new RegistryServerProxy(c)
        Await(reg.cd(Seq("test"), true))

        intercept[Exception] {
          Await(reg.mkDir(""))
        }
      }
    }
  }

  test("creating a registry key with empty name should fail", Tag("empties")) {
    withManager() { m =>
      withClient(m) { c =>

        val reg = new RegistryServerProxy(c)
        Await(reg.cd(Seq("test"), true))

        intercept[Exception] {
          Await(reg.set("", Str("not allowed")))
        }
      }
    }
  }
}
