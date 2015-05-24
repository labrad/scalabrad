package org.labrad

import org.labrad.data._
import org.labrad.util.AsyncSemaphore
import scala.concurrent.{ExecutionContext, Future}

class ContextMgr[T <: ServerContext](factory: => T, handlerFactory: T => RequestContext => Data) {
  @volatile private var firstTime = true
  @volatile private var server: T = _
  @volatile private var handler: RequestContext => Data = _
  private val semaphore = new AsyncSemaphore(1)

  def serve(request: Packet)(implicit ctx: ExecutionContext): Future[Packet] = semaphore.map {
    Server.handle(request) { req =>
      if (firstTime) {
        server = factory
        server.init()
        handler = handlerFactory(server)
        firstTime = false
      }
      handler(req)
    }
  }

  def expire()(implicit ctx: ExecutionContext): Future[Unit] = semaphore.map {
    if (server != null) server.expire
  }
}
