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

import java.lang.reflect.{Method, Modifier}
import java.util.regex.Pattern

import scala.actors.Actor
import scala.actors.Actor._
import scala.collection._

import scala.reflect.ScalaSignature
import scala.tools.scalap._
import scala.tools.scalap.scalax.rules.scalasig._

import grizzled.slf4j.Logging

import Constants._
import annotations.{ServerInfo, Setting}
import annotations.Matchers.NamedMessageHandler
import data._
import data.Conversions._
import errors._
import events._
import util._


class ServerConnection(
    val name: String,
    val info: ServerInfo,
    val server: Server,
    val serverClass: Class[_ <: ServerContext]) extends Connection with Actor {

  val host = Util.getEnv("LABRADHOST", Constants.DEFAULT_HOST)
  val port = Util.getEnvInt("LABRADPORT", Constants.DEFAULT_PORT)
  val password = Util.getEnv("LABRADPASSWORD", Constants.DEFAULT_PASSWORD)
  
  // message handlers
  private var nextMessageId = 1
  def getMessageID = { val id = nextMessageId; nextMessageId += 1; id }
  
  case class Serve(request: Packet)
  case class HandleMessage()
  case object Shutdown

  def act() {
    loop {
      react {
        case Serve(request) =>
          getContextManager(request.context).serveRequest(request)
          
        case Shutdown =>
          // expire all contexts
          contexts.values.map(_.expireFuture).map(_())
    
          // shutdown the server
          server.shutdown
    
          // close our connection to LabRAD
          close
    
          sender ! ()
          exit()
      }
    }
  }
  
  
  
  /**
   * The main serve loop.  This function pulls requests from the incoming
   * queue and serves them.  Note that this function does not return unless
   * the server is interrupted.
   */
  def serve {
    server.connection = this
    server.init

    registerSettings
    subscribeToNamedMessages

    val id = getMessageID
    sendAndWait("Manager") { "S: Notify on Context Expiration" -> (Word(id), Bool(false)) }
    addMessageListener {
      case Message(`id`, context, _, _) =>
        contexts.remove(context).map(_.expire)
    }

    sendAndWait("Manager") { "S: Start Serving" -> Data.EMPTY }
    println("Now serving...")
    
    this.start
  }


  override protected def handleRequest(packet: Packet) {
    this ! Serve(packet)
  }
  
  def triggerShutdown {
    this !? Shutdown
  }

  
  // request serving

  def sendResponse(packet: Packet) { writeQueue.add(packet) }

  /**
   * Map of contexts in which requests have been made and the managers for those contexts.
   */
  private val contexts = mutable.Map.empty[Context, ContextManager]

  /**
   * Get a context manager for the given context.  If this is the first
   * time we have seen a particular context, a new manager will be created.
   */
  private def getContextManager(context: Context) = {
    def handle(id: Long, server: ServerContext, data: Data) =
      getHandler(id)(server, data)
    contexts.getOrElseUpdate(context, new ContextManager(context, handle, sendResponse _))
  }

  /** FIXME this is a hack to allow contexts to communicate */
  def getServerContext(context: Context) = getContextManager(context).server

  /** Get the data required to log in to LabRAD as a server */
  protected def loginData =
    (Word(PROTOCOL), Str(name), Str(info.doc.stripMargin), Str(""))

  /** Get a handler for a particular setting ID */
  def getHandler(id: Long) = dispatchTable(id)
  
  /**
   * Map from setting IDs to methods on the ContextServer that handle
   * those settings.  This table is constructed after logging in but
   * before we begin serving.
   */
  val dispatchTable = ServerConnection.locateSettings(serverClass) // do this when the server is created

  /** Register all settings for which we have handlers */
  private def registerSettings {
    val registrations = for (id <- dispatchTable.keys.toSeq.sorted) yield {
      val handler = dispatchTable(id)
      "S: Register Setting" -> handler.registrationInfo
    }
    sendAndWait("Manager")(registrations: _*)
  }
  
  /** Subscribe to named messages */
  private def subscribeToNamedMessages {
    val subscriptions = for {
      m <- server.getClass.getMethods
      NamedMessageHandler(name) <- m.getDeclaredAnnotations
    } yield {
      val id = getMessageID
      addMessageListener {
        case message @ Message(_, _, `id`, _) =>
          try {
            m.invoke(server, message)
          } catch {
            case e: Exception => e.printStackTrace // TODO meaningful exception handling
          }
      }
      "Subscribe to Named Message" -> Cluster(Str(name), Word(id), Bool(true))
    }
    sendAndWait("Manager")(subscriptions: _*)
  }
}

object ServerConnection extends Logging {
  
  /** Create a new server connection that will use a particular context server object. */
  def apply[T <: ServerContext](server: Server, contextClass: Class[T]) = {
    val (name, inf) = checkAnnotation(server)
    new ServerConnection(name, inf, server, contextClass)
  }
  
  private def checkAnnotation(server: Server) = {
    val cls = server.getClass
    if (!cls.isAnnotationPresent(classOf[ServerInfo]))
      throw new Exception("Server class '%s' needs @ServerInfo annotation.".format(cls.getName))
    val info = cls.getAnnotation(classOf[ServerInfo])
    var name = info.name
    
    // interpolate environment vars
    
    // find all environment vars in the string
    val p = Pattern.compile("%([^%]*)%")
    val m = p.matcher(name)
    val keys = Seq.newBuilder[String]
    while (m.find) keys += m.group(1)
    
    // substitute environment variable into string
    for (key <- keys.result) {
      val value = Util.getEnv(key, null)
      if (value != null) {
        println(key + " -> " + value)
        name = name.replaceAll("%" + key + "%", value)
      }
    }
    
    (name, info)
  }
  
  def locateSettings(clazz: Class[_]): Map[Long, SettingHandler] = {
    // because of overloading, there may be multiple methods with the same name and different
    // calling signatures, all of which need to get dispatched to by the same handler.
    
    val settingMap = mutable.Map.empty[String, Setting]
    val settingsById = mutable.Map.empty[Long, Method]
    val settingsByName = mutable.Map.empty[String, Method]
    
    val settingNames = for {
      m <- clazz.getMethods.toSeq
      if m.isAnnotationPresent(classOf[Setting])
      s = m.getAnnotation(classOf[Setting])
    } yield {
      // setting IDs and names must be unique
      require(!settingsById.contains(s.id), "Multiple settings with id %d".format(s.id))
      require(!settingsByName.contains(s.name), "Multiple settings with name '%s'".format(s.name))
      settingsById(s.id) = m
      settingsByName(s.name) = m

      // only one overload of a method can have @Setting annotation 
      val name = m.getName
      require(!settingMap.contains(name), "Multiple overloads '%s' have @Setting annotation".format(name))
      settingMap(name) = s
      
      name
    }
    
    val methodMap = if (clazz.isAnnotationPresent(classOf[ScalaSignature])) {
      // scala class
      val scalaSig = ScalaSigParser.parse(clazz).get
      val classSymbol = scalaSig.topLevelClasses.find(_.path == clazz.getName).get
      val symbols = classSymbol.children.collect { case m: MethodSymbol => m }
      
      for (name <- settingNames) yield {
        val setting = settingMap(name)
        val methods = clazz.getMethods.filter(_.getName == name).toSeq
        
        def methodNumParams(m: Method) = m.getGenericParameterTypes.size
        
        val nParams = methods map methodNumParams
        assert(Set(nParams.toSeq: _*).size == nParams.size, "overloaded settings must have different numbers of parameters")
        
        def symbolNumParams(ms: MethodSymbol) = ms.infoType match {
          case NullaryMethodType(_) => 0
          case mt: MethodType => mt.paramSymbols.size
        }
        val methodSymbols = symbols.filter(_.name == name).toSeq
        val symbolMap = methodSymbols.map{ ms => symbolNumParams(ms) -> ms }.toMap
        debug("java methods (" + methods.size + "): " + methods.mkString(", "))
        debug("scala methods (" + methodSymbols.size + "): " + methodSymbols.mkString(", "))
        require(methods.size == methodSymbols.size)
        // TODO: make sure the ordering here is consistent
        setting -> (for (m <- methods) yield (m, Some(symbolMap(methodNumParams(m)))))
      }
    } else {
      // java class
      for (name <- settingNames) yield {
        val setting = settingMap(name)
        val methods = clazz.getMethods.filter(_.getName == name).toSeq
        setting -> (for (m <- methods) yield (m, None))
      }
    }
    
    // build handler for each set of overloaded methods and add to the dispatch table
    val dispatchTable = Map.empty[Long, SettingHandler] ++ (
      for ((setting, overloads) <- methodMap) yield {
        setting.id -> SettingHandler.forMethods(setting, overloads)
      }
    )
    
    dispatchTable
    
    /*
    (obj: A) => new Handler {
      def handle(id: Long, data: Data): Data = {
        dispatchTable.get(id) match {
          case Some(handler) =>
            println(handler.setting)
            println(handler.accepts.map(_.tag).mkString (", "))
            println(handler.returns.map(_.tag).mkString(", "))
            if (handler.accepts exists { _.accepts(data.t) })
              handler(obj, data)
            else
              Error(2, "Type not accepted by setting: " + data.t + ". accepted: " + handler.accepts.map(_.tag).mkString(", "))
          case None => Error(1, "Setting not found: " + id)
        }
      }
    }
    */
  }
}

trait Handler {
  def handle(id: Long, data: Data): Data
}
