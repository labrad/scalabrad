package org.labrad.registry

import org.clapper.argot._
import org.clapper.argot.ArgotConverters._
import org.labrad.Client
import org.labrad.RegistryServerProxy
import org.labrad.data.Data
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Migrate {
  def main(args: Array[String]): Unit = {
    val parser = new ArgotParser("migrate-registry")

    val srcOpt = parser.parameter[String]("srchost[:port][/path]", "host, port, and registry path of source manager", false)
    val dstOpt = parser.parameter[String]("dsthost[:port][/path]", "host, port, and registry path of destination manager", false)

    val writeOpt = parser.flag[Boolean](List("w", "write"), "actually write values to destination manager")

    try {
      parser.parse(args)
    } catch {
      case e: ArgotUsageException =>
        println(e.message)
        return
    }

    val (srcHost, srcPort, srcPath) = splitAddr(srcOpt.value.get)
    val (dstHost, dstPort, dstPath) = splitAddr(dstOpt.value.get)
    val write = writeOpt.value.getOrElse(false)

    val stdIn = System.console()

    println(s"Connecting to $srcHost:$srcPort...")
    println(s"Enter password:")
    val srcPass = stdIn.readPassword()
    val srcCxn = new Client(host = srcHost, port = srcPort, password = srcPass)
    srcCxn.connect()

    println(s"Connecting to $dstHost:$dstPort...")
    println(s"Enter password:")
    val dstPass = stdIn.readPassword()
    val dstCxn = new Client(host = dstHost, port = dstPort, password = dstPass)
    dstCxn.connect()

    val srcReg = new RegistryServerProxy(srcCxn)
    val dstReg = new RegistryServerProxy(dstCxn)
    
    def traverse(srcPath: Seq[String], dstPath: Seq[String]): Unit = {
      await(srcReg.cd(srcPath))
      val (dirs, keys) = await(srcReg.dir())

      print(s"fetching ${srcPath.mkString("/")}/")
      val futures = Map.newBuilder[String, Future[Data]]
      val pkt = srcReg.packet()
      pkt.cd(srcPath)
      for (key <- keys.sorted) {
        futures += key -> pkt.get(key)
      }

      val t0 = System.nanoTime()
      await(pkt.send())
      val t1 = System.nanoTime()

      val results = futures.result.map {
        case (key, f) => key -> await(f)
      }

      val t2 = System.nanoTime()
      if (write) {
        val dstPkt = dstReg.packet()
        dstPkt.cd(dstPath, create = true)
        for (key <- keys) {
          dstPkt.set(key, results(key))
        }
        await(dstPkt.send())
      }
      val t3 = System.nanoTime()
      
      println(s" (load: ${((t1-t0) / 1e6).toInt}, save: ${((t3-t2) / 1e6).toInt})")

      for (dir <- dirs.sorted) {
        traverse(srcPath :+ dir, dstPath :+ dir)
      }
    }
    
    val tStart = System.nanoTime()
    traverse(srcPath, dstPath)
    val tEnd = System.nanoTime()
    println(s"total time: ${((tEnd - tStart) / 1e9).toInt} seconds")

    srcCxn.close()
    dstCxn.close()
  }

  /**
   * Wait for a future to complete; shorthand for Await.result
   */
  def await[T](f: Future[T]): T = {
    Await.result(f, 60.seconds)
  }

  // regex for parsing host/port/path combinations
  val Addr = """([\w.]+)((?::\d+)?)((?:/[\w/]*)?)""".r

  /**
   * Split a host:port/path string into component parts,
   * using the provided default port and path if none are given.
   */
  def splitAddr(
    s: String,
    defaultPort: Int = 7682,
    defaultPath: Seq[String] = Seq("")
  ): (String, Int, Seq[String]) = {
    s match {
      case Addr(host, portStr, pathStr) =>
        val port = if (portStr.isEmpty) defaultPort else portStr.stripPrefix(":").toInt
        val path = if (pathStr.isEmpty) defaultPath else splitPath(pathStr)
        (host, port, path)

      case _ => sys.error(s"invalid host address: $s expected host[:port][/path]")
    }
  }

  /**
   * Split a directory path into a form suitable for giving to the registry,
   * with leading empty string to indicate that this is an absolute path.
   */
  def splitPath(s: String): Seq[String] = {
    s.split("/") match {
      case Array() => Seq("")
      case Array(h, tail @ _*) if h.nonEmpty => Seq("", h) ++ tail
      case path => path.toSeq
    }
  }
}
