package org.labrad

import org.labrad.annotations._
import org.labrad.concurrent.Futures._
import org.labrad.data._
import org.labrad.manager.ManagerUtils
import org.labrad.registry._
import org.labrad.types._
import org.labrad.util.Files
import org.scalatest.Tag
import org.scalatest.concurrent.AsyncAssertions
import org.scalatest.fixture
import scala.concurrent.duration._

class RegistryTest extends fixture.FunSuite with AsyncAssertions {

  import ManagerUtils._

  case class FixtureParam(deadline: Deadline)
  implicit def testDeadline(implicit f: FixtureParam) = f.deadline

  def withFixture(test: OneArgTest) = {
    withFixture(test.toNoArgTest(FixtureParam(Deadline.now + 1.minute)))
  }

  def testAllBackends(testName: String)(func: (RegistryStore, Boolean) => Unit): Unit = {
    test(s"BinaryFileStore: $testName") { implicit f =>
      Files.withTempDir { dir => func(new BinaryFileStore(dir), true) }
    }

    test(s"DelphiFileStore: $testName", Tag("delphi")) { implicit f =>
      Files.withTempDir { dir => func(new DelphiFileStore(dir), false) }
    }

    test(s"SQLiteStore: $testName") { implicit f =>
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

  test("registry can store and retrieve arbitrary data") { implicit f =>
    withManager() { m =>
      withClient(m) { c =>
        c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")).await()
        try {
          for (i <- 0 until 1000) {
            val tpe = Hydrant.randomType
            val data = Hydrant.randomData(tpe)
            c.send("Registry", "set" -> Cluster(Str("a"), data)).await()
            val resp = c.send("Registry", "get" -> Str("a")).await().apply(0)
            c.send("Registry", "del" -> Str("a")).await()
            assert(resp == data, s"${resp} (type=${resp.t}) is not equal to ${data} (type=${data.t})")
          }
        } finally {
          c.send("Registry", "cd" -> Str(".."), "rmdir" -> Str("test")).await()
        }
      }
    }
  }

  test("registry can deal with unicode and strange characters in directory names", Tag("chars")) { implicit f =>
    withManager() { m =>
      withClient(m) { c =>
        val dir = "<\u03C0|\u03C1>??+*\\/:|"
        c.send("Registry", "mkdir" -> Str(dir)).await()
        val (dirs, _) = c.send("Registry", "dir" -> Data.NONE).await().apply(0).get[(Seq[String], Seq[String])]
        assert(dirs contains dir)
        c.send("Registry", "cd" -> Str(dir)).await()
      }
    }
  }

  test("registry can deal with unicode and strange characters in key names", Tag("chars")) { implicit f =>
    withManager() { m =>
      withClient(m) { c =>
        val key = "<\u03C0|\u03C1>??+*\\/:|"
        val data = Str("Hello!")
        c.send("Registry", "set" -> Cluster(Str(key), Str("Hello!"))).await()
        val (_, keys) = c.send("Registry", "dir" -> Data.NONE).await().apply(0).get[(Seq[String], Seq[String])]
        assert(keys contains key)
        val result = c.send("Registry", "get" -> Str(key)).await().apply(0)
        assert(result == data)
      }
    }
  }

  test("registry cd with no arguments stays in same directory") { implicit f =>
    withManager() { m =>
      withClient(m) { c =>
        c.send("Registry", "cd" -> Cluster(Arr(Str("test"), Str("a")), Bool(true))).await()
        val result = c.send("Registry", "cd" -> Data.NONE).await().apply(0)
        assert(result.get[Seq[String]] == Seq("", "test", "a"))
      }
    }
  }

  test("registry sends message when key is created") { implicit f =>
    withManager() { m =>
      withClient(m) { c =>

        val w = new Waiter

        val msgId = 1234

        c.addMessageListener {
          case Message(src, ctx, `msgId`, data) =>
            w { assert(data == Cluster(Str("a"), Bool(false), Bool(true))) }
            w.dismiss
        }

        c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")).await()
        c.send("Registry", "Notify on Change" -> Cluster(UInt(msgId), Bool(true))).await()
        c.send("Registry", "set" -> Cluster(Str("a"), Str("test"))).await()
        w.await(timeout(10.seconds))
      }
    }
  }

  test("registry sends message when key is changed") { implicit f =>
    withManager() { m =>
      withClient(m) { c =>

        val w = new Waiter

        val msgId = 1234

        c.addMessageListener {
          case Message(src, ctx, `msgId`, data) =>
            w { assert(data == Cluster(Str("a"), Bool(false), Bool(true))) }
            w.dismiss
        }

        c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")).await()
        c.send("Registry", "set" -> Cluster(Str("a"), Str("first"))).await()
        c.send("Registry", "Notify on Change" -> Cluster(UInt(msgId), Bool(true))).await()
        c.send("Registry", "set" -> Cluster(Str("a"), Str("second"))).await()
        w.await
      }
    }
  }

  test("registry sends message when key is deleted") { implicit f =>
    withManager() { m =>
      withClient(m) { c =>

        val w = new Waiter

        val msgId = 1234

        c.addMessageListener {
          case Message(src, ctx, `msgId`, data) =>
            w { assert(data == Cluster(Str("a"), Bool(false), Bool(false))) }
            w.dismiss
        }

        c.send("Registry", "mkdir" -> Str("test"), "cd" -> Str("test")).await()
        c.send("Registry", "set" -> Cluster(Str("a"), Str("first"))).await()
        c.send("Registry", "Notify on Change" -> Cluster(UInt(msgId), Bool(true))).await()
        c.send("Registry", "del" -> Str("a")).await()
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

  test("deleting a registry directory containing a dir should fail") { implicit f =>
    withManager() { m =>
      withClient(m) { c =>

        val reg = new RegistryServerProxy(c)
        reg.cd(Seq("a", "b"), true).await()

        def dirs(): Seq[String] = {
          val (ds, ks) = reg.dir().await()
          ds
        }

        reg.cd("..").await()
        assert(dirs() contains "b")

        reg.cd("..").await()
        assert(dirs() contains "a")

        intercept[Exception] {
          reg.rmDir("a").await()
        }
      }
    }
  }

  test("deleting a registry directory containing a key should fail") { implicit f =>
    withManager() { m =>
      withClient(m) { c =>

        val reg = new RegistryServerProxy(c)
        reg.cd(Seq("a"), true).await()
        reg.set("blah", Str("test")).await()

        val (_, keys) = reg.dir().await()
        assert(keys contains "blah")

        reg.cd("..").await()

        intercept[Exception] {
          reg.rmDir("a").await()
        }
      }
    }
  }

  test("creating a registry directory with empty name should fail", Tag("empties")) { implicit f =>
    withManager() { m =>
      withClient(m) { c =>

        val reg = new RegistryServerProxy(c)
        reg.cd(Seq("test"), true).await()

        intercept[Exception] {
          reg.mkDir("").await()
        }
      }
    }
  }

  test("creating a registry key with empty name should fail", Tag("empties")) { implicit f =>
    withManager() { m =>
      withClient(m) { c =>

        val reg = new RegistryServerProxy(c)
        reg.cd(Seq("test"), true).await()

        intercept[Exception] {
          reg.set("", Str("not allowed")).await()
        }
      }
    }
  }
}
