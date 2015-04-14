package org.labrad

import java.util.concurrent.atomic.AtomicInteger
import org.labrad.data._
import org.labrad.errors.LabradException
import org.labrad.util.{Logging, Pool}
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

class RequestDispatcher(sendFunc: Packet => Unit) extends Logging {
  private val nextId = new AtomicInteger(1)
  private val idPool = new Pool(nextId.getAndIncrement())
  private val promises = mutable.Map.empty[Int, Promise[Seq[Data]]]

  def startRequest(request: Request): Future[Seq[Data]] = {
    val promise = Promise[Seq[Data]]
    val id = idPool.get
    synchronized {
      promises(id) = promise
    }
    sendFunc(Packet.forRequest(request, id))
    promise.future
  }

  def finishRequest(packet: Packet): Unit = {
    val id = -packet.id
    val promiseOpt = synchronized {
      promises.get(id).map { promise =>
        promises -= id
        idPool.release(id)
        promise
      }
    }
    promiseOpt match {
      case Some(promise) =>
        val results = packet.records.map(_.data)
        results.find(_.isError) match {
          case Some(error) => promise.failure(LabradException(error))
          case None => promise.success(results)
        }
      case None =>
        log.warn(s"Received a response to an unknown request: $id")
    }
  }

  def failAll(cause: Throwable): Unit = synchronized {
    val ps = synchronized {
      val ps = for ((id, promise) <- promises) yield {
        idPool.release(id)
        promise
      }
      promises.clear
      ps
    }
    ps.foreach(_.failure(cause))
  }
}
