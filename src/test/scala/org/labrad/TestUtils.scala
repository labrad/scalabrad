package org.labrad

import java.io.File
import java.nio.ByteOrder
import java.util.Date
import org.labrad.data._
import org.labrad.manager.CentralNode
import org.labrad.registry._
import org.labrad.types._
import org.labrad.util.{Logging, Util}
import org.scalatest.FunSuite
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

object TestUtils extends {

  def await[A](future: Future[A]): A = Await.result(future, 30.seconds)

  def withManager[T](f: (String, Int, Array[Char]) => T): T = {
    val host = "localhost"
    val port = 10000 + Random.nextInt(50000) //Util.findAvailablePort()
    val password = "testPassword12345!@#$%".toCharArray

    val registryFile = File.createTempFile("labrad-registry", "")

    val registryStore = SQLiteStore(registryFile)

    val manager = new CentralNode(port, password, Some(registryStore))
    try {
      f(host, port, password)
    } finally {
      manager.stop()
    }
  }

  def withClient[A](host: String, port: Int, password: Array[Char])(func: Client => A): A = {
    val c = new Client(host = host, port = port, password = password)
    c.connect()
    try {
      func(c)
    } finally {
      try c.close() catch { case _: Throwable => }
    }
  }

  def withServer[T](host: String, port: Int, password: Array[Char])(body: TestSrv => T) = {
    val s = new TestSrv
    s.start(host, port, password)
    try {
      body(s)
    } finally {
      try s.stop() catch { case _: Throwable => }
    }
  }
}
