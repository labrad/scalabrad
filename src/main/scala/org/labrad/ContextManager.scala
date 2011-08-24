package org.labrad

import scala.actors.Actor
import scala.actors.Actor._
import scala.collection._
import scala.util.control.Breaks

import java.io.{PrintWriter, StringWriter}

import data._
import errors.LabradException


class ContextManager(context: Context, handle: (Long, ServerContext, Data) => Data, send: Packet => Unit) extends Actor {
  var server: ServerContext = _
  
  case class Serve(request: Packet)
  case object Expire
  
  def act() {
    val breaks = new Breaks
    import breaks.{break, breakable}
    var shouldInit = true
    
    loop {
      react {
        case Serve(request) =>
          val response = Seq.newBuilder[Record]          
          breakable {
            for (Record(id, data) <- request.records) {
              val respData = try {
                if (shouldInit) {
                  server.init
                  shouldInit = false
                }
                server.source = request.target
                handle(id, server, data)
              } catch {
                case ex: LabradException => ex.toData
                case ex: Throwable => errorFor(ex)
              }
              response += Record(id, respData)
              if (respData.isError) break
            }
          }
          
          // send response
          send(Packet(-request.id, request.target, request.context, response.result))
          
        case Expire =>
          server.expire
          sender ! ()
          exit()
      }
    }
  }
  
  // automatically start this actor
  start
  
  def serveRequest(request: Packet) {
    this ! Serve(request)
  }

  def expire {
    this !? Expire
  }
  
  def expireFuture = this !! Expire
  
  /**
   * Turn a generic exception into an error record.
   * @param ex
   * @return
   */
  private def errorFor(ex: Throwable) = {
    val sw = new StringWriter
    ex.printStackTrace(new PrintWriter(sw))
    Error(0, sw.toString)
  }
}

