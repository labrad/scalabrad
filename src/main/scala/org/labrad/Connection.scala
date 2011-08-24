/*
 * Copyright 2008 Matthew Neeley
 *
 * This file is part of JLabrad.
 *
 * JLabrad is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * JLabrad is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with JLabrad.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.labrad

import java.awt.EventQueue
import java.io.IOException
import java.net.Socket
import java.security.MessageDigest
import java.util.concurrent.{BlockingQueue, Callable, ExecutionException, Executors, Future, LinkedBlockingQueue}

import data._
import errors._
import events.MessageListener
import util.LookupProvider


trait Connection {
  
  val name: String
  val host: String
  val port: Int
  def password: String
  
  var id: Long = _
  var loginMessage: String = _
    
  private var isConnected = false
  def connected = isConnected
  private def connected_=(state: Boolean) { isConnected = state }
  
  // events
  protected var messageListeners: List[PartialFunction[Message, Unit]] = Nil

  def addMessageListener(listener: PartialFunction[Message, Unit]) { messageListeners ::= listener }
  def removeMessageListener(listener: PartialFunction[Message, Unit]) {
    messageListeners = messageListeners filterNot (_ == listener)
  }
  
  
  protected var requestDispatcher: RequestDispatcher = _
  protected var writeQueue: BlockingQueue[Packet] = _
  protected val lookupProvider = new LookupProvider(this)
  protected val executor = Executors.newCachedThreadPool
  
  
  // networking stuff
  private var socket: Socket = _
  private var reader: Thread = _
  private var writer: Thread = _
  
  /**
   * Connect to the LabRAD manager.
   * @throws IOException if a network error occurred
   * @throws IncorrectPasswordException if the password was not correct
   * @throws LoginFailedException if the login failed for some other reason
   */
  def connect {
    socket = new Socket(host, port)
    socket.setTcpNoDelay(true)
    socket.setKeepAlive(true)
    val inputStream = new PacketInputStream(socket.getInputStream)
    val outputStream = new PacketOutputStream(socket.getOutputStream)

    writeQueue = new LinkedBlockingQueue[Packet]
    requestDispatcher = new RequestDispatcher(packet => writeQueue.add(packet))

    reader = new Thread(new Runnable {
      def run {
        try {
          while (!Thread.interrupted)
            handlePacket(inputStream.readPacket)
        } catch {
          case e: IOException => if (!Thread.interrupted) close(e)
          case e: Exception => close(e)
        }
      }
    }, "Packet Reader Thread")

    writer = new Thread(new Runnable {
      def run {
        try {
          while (true) {
            val p = writeQueue.take
            outputStream.writePacket(p)
          }
        } catch {
          case e: InterruptedException => // this happens when the connection is closed.
          case e: IOException => close(e) // let the client know that we have disconnected.
          case e: Exception => close(e)
        }
      }
    }, "Packet Writer Thread")

    reader.start
    writer.start

    try {
      // we set connected to true temporarily so that login requests will complete
      // however, we do not use the usual setter since that would send a message
      // to interested parties
      isConnected = true
      doLogin(password)
    } catch {
      case e: LoginFailedException => close(e); throw e
      case e: IncorrectPasswordException => close(e); throw e
    } finally {
      isConnected = false
    }
    connected = true
  }
  
  
  /**
   * Logs in to LabRAD using the standard protocol.
   * @param password
   * @throws IncorrectPasswordException if the password was not correct
   * @throws LoginFailedException if the login failed for some other reason
   */
  private def doLogin(password: String) {
    val mgr = Constants.MANAGER

    try {
      // send first ping packet
      val Bytes(challenge) = sendAndWait(Request(mgr))(0)

      // get password challenge
      val md = MessageDigest.getInstance("MD5")
      md.update(challenge)
      md.update(password.getBytes(Data.STRING_ENCODING))

      // send password response
      val data = Bytes(md.digest)
      val Str(msg) = try {
        sendAndWait(Request(mgr, records = Seq(Record(0, data))))(0)
      } catch {
        case e: ExecutionException => throw new IncorrectPasswordException
      }

      // get welcome message
      loginMessage = msg

      // send identification packet
      val Word(assignedId) = sendAndWait(Request(mgr, records = Seq(Record(0, loginData))))(0)
      id = assignedId
      
    } catch {
      case e: InterruptedException => throw new LoginFailedException(e)
      case e: ExecutionException => throw new LoginFailedException(e)
      case e: IOException => throw new LoginFailedException(e)
    }
  }
  
  protected def loginData: Data
    
  /** Closes the network connection to LabRAD */
  def close { close(new IOException("Connection closed.")) }


  /** Closes the connection to LabRAD after an error */
  private def close(cause: Throwable) = synchronized {
    if (connected) {
      // set our status as closed
      connected = false

      // shutdown the lookup service
      executor.shutdown

      // cancel all pending requests
      requestDispatcher.failAll(cause)

      // interrupt the writer thread
      writer.interrupt
      try { writer.join }
      catch { case e: InterruptedException => }

      // interrupt the reader thread
      reader.interrupt
      // this doesn't actually kill the thread, because it is blocked
      // on a stream read.  To kill the reader, we close the socket.
      try { socket.close } catch { case e: IOException => }
      try { reader.join } catch { case e: InterruptedException => }
    }
  }
  
  
  /** Low word of next context that will be created. */
  private var nextContext = 0L
  private val contextLock = new Object

  /**
   * Create a new context for this connection.
   * @return
   */
  def newContext = contextLock synchronized {
    nextContext += 1
    new Context(0, nextContext)
  }
  
  
  // sending requests
  
  def send(target: String, context: Context = Context(0, 0))
          (records: (String, Data)*): () => Seq[Data] = {
    val recs = for ((name, data) <- records) yield NameRecord(name, data)
    sendRequest(NameRequest(target, context, recs))
  }
  
  def sendAndWait(target: String, context: Context = Context(0, 0))
          (records: (String, Data)*): Seq[Data] =
    send(target, context)(records: _*)()
  
  
  def sendRequest(request: NameRequest): () => Seq[Data] =
    lookupProvider.doLookupsFromCache(request) match {
      case Some(request) => sendWithoutLookups(request)
      case None => {
        val callable = new Callable[Seq[Data]] {
          def call: Seq[Data] =
            sendWithoutLookups(lookupProvider.doLookups(request))()
        }
        val future = executor.submit(callable)
        () => future.get
      }
    }

  def sendAndWait(request: NameRequest) = sendRequest(request)()

  
  def sendRequest(request: Request): () => Seq[Data] = sendWithoutLookups(request)
  def sendAndWait(request: Request): Seq[Data] = sendRequest(request)()
  

  private def sendWithoutLookups(request: Request): () => Seq[Data] = {
    require(connected, "Not connected.")
    requestDispatcher.startRequest(request)
  }


  def sendMessage(request: NameRequest) {
    sendMessageWithoutLookups(lookupProvider.doLookups(request))
  }
  
  private def sendMessageWithoutLookups(request: Request) {
    require(connected, "Not connected.")
    writeQueue.add(Packet.forMessage(request))
  }

  
  
  /** Handle packets coming in from the wire */
  protected def handlePacket(packet: Packet) {
    packet match {
      case Packet(id, _, _, _) if id < 0 => handleResponse(packet)
      case Packet(0, _, _, _) => handleMessage(packet)
      case _ => handleRequest(packet)
    }
  }

  protected def handleResponse(packet: Packet) {
    requestDispatcher.finishRequest(packet)
  }
  
  protected def handleMessage(packet: Packet) {
    EventQueue.invokeLater(new Runnable {
      def run {
        for (Record(id, data) <- packet.records) {
          val message = Message(packet.target, packet.context, id, data)
          for (listener <- messageListeners if listener.isDefinedAt(message))
            listener(message)
        }
      }
    })
  }
  
  protected def handleRequest(packet: Packet) {}
}

