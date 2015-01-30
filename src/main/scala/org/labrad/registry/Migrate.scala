package org.labrad.registry

import org.labrad.Client
import org.labrad.RegistryServerProxy
import org.labrad.data.Data
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Migrate {
  def await[T](f: Future[T]): T = {
    Await.result(f, 60.seconds)
  }

  def splitHostPort(s: String, defaultPort: Int = 7682): (String, Int) = {
    s.split(":") match {
      case Array(host) => host -> defaultPort
      case Array(host, port) => host -> port.toInt
    }
  }

  def splitDir(s: String): Seq[String] = {
    s.split("/") match {
      case Array() => Seq("")
      case Array(h, tail @ _*) if h.nonEmpty => Seq("", h) ++ tail
      case path => path.toSeq
    }
  }

  def main(args: Array[String]): Unit = {
    val Array(src, srcDir, dst, dstDir) = args

    val (srcHost, srcPort) = splitHostPort(src)
    val (dstHost, dstPort) = splitHostPort(dst)

    val srcPath = splitDir(srcDir)
    val dstPath = splitDir(dstDir)

    val stdIn = System.console()

    println(s"Enter password for $src:")
    val srcPass = stdIn.readPassword()

    println(s"Enter password for $dst:")
    val dstPass = stdIn.readPassword()

    val srcCxn = new Client(host = srcHost, port = srcPort, password = srcPass)
    val dstCxn = new Client(host = dstHost, port = dstPort, password = dstPass)

    srcCxn.connect()
    dstCxn.connect()

    val srcReg = new RegistryServerProxy(srcCxn)
    val dstReg = new RegistryServerProxy(dstCxn)

    val srcCtx = srcCxn.newContext
    val dstCtx = dstCxn.newContext

    def traverse(srcPath: Seq[String], dstPath: Seq[String]): Unit = {
      await(srcReg.cd(srcPath))
      val (dirs, keys) = await(srcReg.dir())
      for (dir <- dirs) {
        println(s"entering ${srcPath.mkString("/")}/$dir/")
        traverse(srcPath :+ dir, dstPath :+ dir)
      }

      val futures = Map.newBuilder[String, Future[Data]]

      val pkt = srcReg.packet(srcCtx)
      pkt.cd(srcPath)
      for (key <- keys) {
        println(s"fetching ${srcPath.mkString("/")}/$key")
        futures += key -> pkt.get(key)
      }
      await(pkt.send)

      val results = futures.result.map {
        case (key, f) => key -> await(f)
      }

      val dstPkt = dstReg.packet(dstCtx)
      dstPkt.cd(dstPath, create = true)
      for (key <- keys) {
        dstPkt.set(key, results(key))
      }
      await(dstPkt.send)
    }
    traverse(srcPath, dstPath)

    srcCxn.close()
    dstCxn.close()
  }
}
