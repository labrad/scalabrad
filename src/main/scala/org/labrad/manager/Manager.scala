package org.labrad.manager

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import org.labrad.annotations._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.registry._
import org.labrad.util._
import org.labrad.util.akka.{TypedProps, TypedActor}
import scala.annotation.tailrec
import scala.concurrent.Await
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


class CentralNode(port: Int, password: String, registryRoot: File, remotePort: Int) extends Logging {
  val config = ConfigFactory.parseString(s"""
    akka {
      loglevel = ERROR
      actor {
        provider = "akka.remote.RemoteActorRefProvider"
      }
      remote {
        transport = "akka.remote.netty.NettyRemoteTransport"
        netty {
          hostname = "127.0.0.1"
          port = $remotePort
        }
      }
    }
  """)
  val system = ActorSystem("Labrad", config)
  val ta = TypedActor(system)

  // start services
  val tracker = ta.typedActorOf(TypedProps[StatsTracker](new StatsTrackerImpl), "stats")
  val hub = ta.typedActorOf(TypedProps[Hub](new HubImpl(tracker, () => messager)), "hub")
  val messager: Messager = ta.typedActorOf(TypedProps[Messager](new MessagerImpl(hub)), "messager")
  val auth = ta.typedActorOf(TypedProps[AuthService](new AuthServiceImpl(password)), "auth")

  tracker.connectServer(Manager.ID, Manager.NAME)

  { // connect registry as server 2L
    val name = "Registry"
    val id = hub.allocateServerId(name)
    val actor = ta.typedActorOf(
      TypedProps[ServerActor](new Registry(id, name, registryRoot, hub, tracker)),
      name
    )
    hub.connectServer(id, name, actor)
  }

  // start listening for incoming network connections
  val listener = new Listener(auth, hub, tracker, messager, port)(system.dispatcher)

  def stop() {
    listener.stop()
    system.shutdown()
    system.awaitTermination()
  }
}


class OuterNode(port: Int, hubHost: String, hubPort: Int, remotePort: Int) {
  val config = ConfigFactory.parseString(s"""
    akka {
      loglevel = ERROR
      actor {
        provider = "akka.remote.RemoteActorRefProvider"
      }
      remote {
        transport = "akka.remote.netty.NettyRemoteTransport"
        netty {
          hostname = "$hubHost"
          port = $remotePort
        }
      }
    }
  """)
  val system = ActorSystem("Labrad", config)
  val ta = TypedActor(system)

  private def actorUrl(path: String) = s"akka://Labrad@$hubHost:$hubPort/user/$path"
  private def actorRef(path: String): ActorRef = {
    val refF = system.actorSelection(actorUrl(path)).resolveOne(10.seconds)
    Await.result(refF, 11.seconds)
  }

  // proxies for remote actors on the central manager
  val tracker = ta.typedActorOf(TypedProps[StatsTracker], actorRef("stats"))
  val messager = ta.typedActorOf(TypedProps[Messager], actorRef("messager"))
  val centralHub = ta.typedActorOf(TypedProps[Hub], actorRef("hub"))
  val auth = ta.typedActorOf(TypedProps[AuthService], actorRef("auth"))

  // local hub for routing packets
  val hub = ta.typedActorOf(TypedProps[Hub](new RemoteHubImpl(centralHub, tracker)), "hub")

  // start listening for incoming connections
  val listener = new Listener(auth, hub, tracker, messager, port)(system.dispatcher)

  def stop() {
    listener.stop()
    system.shutdown()
    system.awaitTermination()
  }
}


object Manager extends Logging {
  val ID = 1L
  val NAME = "Manager"
  val DOC = "the Manager"

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
      case "--remotePort" :: remotePort :: rest => parseArgs(rest) + ("remotePort" -> remotePort)
      case arg :: rest => sys.error("Unknown argument: " + arg)
    }
    val options = parseArgs(args.toList)

    val port = options.get("port").orElse(sys.env.get("LABRADPORT")).map(_.toInt).getOrElse(7682)
    val remotePort = options.get("remotePort").map(_.toInt).getOrElse(7676)
    val password = options.get("password").orElse(sys.env.get("LABRADPASSWORD")).getOrElse("")
    val registry = options.get("registry").orElse(sys.env.get("LABRADREGISTRY")).map(new File(_)).getOrElse(sys.props("user.home") / ".labrad" / "registry")

    val centralNode = new CentralNode(port, password, registry, remotePort)

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


object Node extends Logging {
  def main(args: Array[String]) {
    def parseArgs(args: List[String]): Map[String, String] = args match {
      case Nil => Map()
      case "--port" :: port :: rest => parseArgs(rest) + ("port" -> port)
      case arg :: rest => sys.error("Unknown argument: " + arg)
    }
    val options = parseArgs(args.toList)
    val port = options.get("port").orElse(sys.env.get("LABRADPORT")).map(_.toInt).getOrElse(7782)

    val outerNode = new OuterNode(port, "127.0.0.1", hubPort = 7676, remotePort = 7677)
  }
}
