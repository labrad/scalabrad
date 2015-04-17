package org.labrad

import java.io.IOException
import java.net.Socket
import java.nio.ByteOrder
import java.nio.CharBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.concurrent.{ExecutionException, Executors}
import org.labrad.data._
import org.labrad.errors._
import org.labrad.events.MessageListener
import org.labrad.manager.Manager
import org.labrad.util.{Counter, LookupProvider}
import scala.concurrent.{Await, Channel, ExecutionContext, Future}
import scala.concurrent.duration._

trait Connection {

  val name: String
  val host: String
  val port: Int
  def password: Array[Char]

  var id: Long = _
  var loginMessage: String = _

  private var isConnected = false
  def connected = isConnected
  private def connected_=(state: Boolean): Unit = { isConnected = state }

  protected var connectionListeners: List[PartialFunction[Boolean, Unit]] = Nil
  protected var messageListeners: List[PartialFunction[Message, Unit]] = Nil

  def addConnectionListener(listener: PartialFunction[Boolean, Unit]): Unit = {
    connectionListeners ::= listener
  }
  def removeConnectionListener(listener: PartialFunction[Boolean, Unit]): Unit = {
    connectionListeners = connectionListeners filterNot (_ == listener)
  }

  def addMessageListener(listener: PartialFunction[Message, Unit]): Unit = {
    messageListeners ::= listener
  }
  def removeMessageListener(listener: PartialFunction[Message, Unit]): Unit = {
    messageListeners = messageListeners filterNot (_ == listener)
  }

  protected val writeQueue: Channel[Packet] = new Channel[Packet]
  protected val lookupProvider = new LookupProvider(this.send)
  protected val requestDispatcher = new RequestDispatcher(sendPacket)
  protected val executor = Executors.newCachedThreadPool
  implicit val executionContext = ExecutionContext.fromExecutor(executor)

  private var socket: Socket = _
  private var reader: Thread = _
  private var writer: Thread = _

  def connect(): Unit = {
    socket = new Socket(host, port)
    socket.setTcpNoDelay(true)
    socket.setKeepAlive(true)
    implicit val byteOrder = ByteOrder.BIG_ENDIAN
    val inputStream = new PacketInputStream(socket.getInputStream)
    val outputStream = new PacketOutputStream(socket.getOutputStream)

    reader = new Thread(new Runnable {
      def run: Unit = {
        try {
          while (!Thread.interrupted)
            handlePacket(inputStream.readPacket)
        } catch {
          case e: IOException => if (!Thread.interrupted) close(e)
          case e: Exception => close(e)
        }
      }
    }, s"${name}-reader")

    writer = new Thread(new Runnable {
      def run: Unit = {
        try {
          while (true)
            outputStream.writePacket(writeQueue.read)
        } catch {
          case e: InterruptedException => // connection closed
          case e: IOException => close(e)
          case e: Exception => close(e)
        }
      }
    }, s"${name}-writer")

    reader.setDaemon(true)
    writer.setDaemon(true)

    reader.start()
    writer.start()

    connected = true
    try {
      doLogin(password)
    } catch {
      case e: Throwable => close(e); throw e
    }

    for (listener <- connectionListeners) listener.lift(true)
  }

  private def doLogin(password: Array[Char]): Unit = {
    try {
      // send first ping packet; response is password challenge
      val Bytes(challenge) = Await.result(send(Request(Manager.ID)), 10.seconds)(0)

      val md = MessageDigest.getInstance("MD5")
      md.update(challenge)
      md.update(UTF_8.encode(CharBuffer.wrap(password)))
      val data = Bytes(md.digest)

      // send password response; response is welcome message
      val Str(msg) = try {
        Await.result(send(Request(Manager.ID, records = Seq(Record(0, data)))), 10.seconds)(0)
      } catch {
        case e: ExecutionException => throw new IncorrectPasswordException
      }
      loginMessage = msg

      // send identification packet; response is our assigned connection id
      val UInt(assignedId) = Await.result(send(Request(Manager.ID, records = Seq(Record(0, loginData)))), 10.seconds)(0)
      id = assignedId

    } catch {
      case e: InterruptedException => throw new LoginFailedException(e)
      case e: ExecutionException => throw new LoginFailedException(e)
      case e: IOException => throw new LoginFailedException(e)
    }
  }

  protected def loginData: Data

  def close(): Unit = close(new IOException("Connection closed."))

  private def close(cause: Throwable): Unit = {
    val listeners = synchronized {
      if (connected) {
        connected = false
        requestDispatcher.failAll(cause)
        executor.shutdown

        writer.interrupt
        reader.interrupt

        connectionListeners.map(_.lift)
      } else {
        Seq()
      }
    }
    for (listener <- listeners) listener(false)

    try { writer.join } catch { case e: InterruptedException => }

    try { socket.close } catch { case e: IOException => }
    try { reader.join } catch { case e: InterruptedException => }
  }

  def send(target: String, records: (String, Data)*): Future[Seq[Data]] =
    send(target, Context(0, 0), records: _*)

  def send(target: String, context: Context, records: (String, Data)*): Future[Seq[Data]] = {
    val recs = for ((name, data) <- records) yield NameRecord(name, data)
    send(NameRequest(target, context, recs))
  }

  def send(request: NameRequest): Future[Seq[Data]] =
    lookupProvider.resolve(request).flatMap(send)

  def send(request: Request): Future[Seq[Data]] = {
    require(connected, "Not connected.")
    requestDispatcher.startRequest(request)
  }

  def sendMessage(request: NameRequest): Unit =
    lookupProvider.resolve(request).map(r => sendPacket(Packet.forMessage(r)))

  protected def sendPacket(packet: Packet): Unit = {
    require(connected, "Not connected.")
    writeQueue.write(packet)
  }


  private val contextCounter = new Counter(0, 0xFFFFFFFFL)
  def newContext = Context(0, contextCounter.next)

  protected def handlePacket(packet: Packet): Unit = packet match {
    case Packet(id, _, _, _) if id > 0 => handleRequest(packet)
    case Packet( 0, _, _, _)           => handleMessage(packet)
    case Packet(id, _, _, _) if id < 0 => handleResponse(packet)
  }

  protected def handleResponse(packet: Packet): Unit = {
    requestDispatcher.finishRequest(packet)
  }

  protected def handleMessage(packet: Packet): Unit = {
    executionContext.execute(new Runnable {
      def run: Unit = {
        for (Record(id, data) <- packet.records) {
          val message = Message(packet.target, packet.context, id, data)
          messageListeners.foreach(_.lift(message))
        }
      }
    })
  }

  protected def handleRequest(packet: Packet): Unit = {}
}
