package org.labrad.manager

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import org.labrad.annotations._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.registry._
import org.labrad.util._
import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


trait AuthService {
  def authenticate(challenge: Array[Byte], response: Array[Byte]): Boolean
}

class AuthServiceImpl(password: String) extends AuthService {
  def authenticate(challenge: Array[Byte], response: Array[Byte]): Boolean = {
    val md = MessageDigest.getInstance("MD5")
    md.update(challenge)
    md.update(password.getBytes(UTF_8))
    val expected = md.digest
    var same = expected.length == response.length
    for ((a, b) <- expected zip response) same = same & (a == b)
    same
  }
}


class CentralNode(port: Int, password: String, registryRoot: File) extends Logging {
  // start services
  val tracker = new StatsTrackerImpl
  val hub: Hub = new HubImpl(tracker, () => messager)
  val messager: Messager = new MessagerImpl(hub)
  val auth: AuthService = new AuthServiceImpl(password)

  // Manager gets id 1L
  tracker.connectServer(Manager.ID, Manager.NAME)

  { // Registry gets id 2L
    val name = "Registry"
    val id = hub.allocateServerId(name)
    val server = new Registry(id, name, registryRoot, hub, tracker)
    hub.connectServer(id, name, server)
  }

  // start listening for incoming network connections
  val listener = new Listener(auth, hub, tracker, messager, port)

  def stop() {
    listener.stop()
  }
}


object Manager extends Logging {
  val ID = 1L
  val NAME = "Manager"
  val DOC = "Provides basic support for all labrad connections, including discovery of other servers and lookup of metadata about them."

  // setting ids
  val SERVERS = 1L
  val SETTINGS = 2L
  val LOOKUP = 3L

  implicit class PathString(path: String) {
    def / (file: String): File = new File(path, file)
  }

  implicit class PathFile(path: File) {
    def / (file: String): File = new File(path, file)
  }

  def main(args: Array[String]) {
    def parseArgs(args: List[String]): Map[String, String] = args match {
      case Nil => Map()
      case "--password" :: password :: rest => parseArgs(rest) + ("password" -> password)
      case "--port" :: port :: rest => parseArgs(rest) + ("port" -> port)
      case "--registry" :: registry :: rest => parseArgs(rest) + ("registry" -> registry)
      case arg :: rest => sys.error("Unknown argument: " + arg)
    }
    val options = parseArgs(args.toList)

    val port = options.get("port").orElse(sys.env.get("LABRADPORT")).map(_.toInt).getOrElse(7682)
    val password = options.get("password").orElse(sys.env.get("LABRADPASSWORD")).getOrElse("")
    val registry = options.get("registry").orElse(sys.env.get("LABRADREGISTRY")).map(new File(_)).getOrElse(sys.props("user.home") / ".labrad" / "registry")

    val centralNode = new CentralNode(port, password, registry)

    @tailrec def enterPressed(): Boolean =
      System.in.available > 0 && (System.in.read() == '\n'.toInt || enterPressed())

    var done = false
    while (!done) {
      Thread.sleep(100)
      if (enterPressed()) done = true
    }
    centralNode.stop()
  }
}
