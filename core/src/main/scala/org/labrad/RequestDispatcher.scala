package org.labrad

import java.util.concurrent.atomic.AtomicInteger
import org.labrad.data._
import org.labrad.errors.LabradException
import org.labrad.util.Logging
import scala.collection.mutable
import scala.concurrent.{Future, Promise}

class RequestDispatcher(sendFunc: Packet => Unit) extends Logging {
  private val _nextId = new AtomicInteger(1)
  private def nextId = _nextId.getAndIncrement()
  private val idPool = mutable.Queue.empty[Int]
  private val promises = mutable.Map.empty[Int, Promise[Seq[Data]]]

  def startRequest(request: Request): Future[Seq[Data]] = {
    val promise = Promise[Seq[Data]]
    val id = synchronized {
      val id = if (idPool.isEmpty) nextId else idPool.dequeue
      promises(id) = promise
      id
    }
    sendFunc(Packet.forRequest(request, id))
    promise.future
  }

  def finishRequest(packet: Packet): Unit = {
    val id = -packet.id
    val promiseOpt = synchronized {
      promises.get(id).map { promise =>
        promises -= id
        idPool += id
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

  def failAll(cause: Throwable): Unit = {
    val ps = synchronized {
      val ps = for ((id, promise) <- promises) yield {
        idPool += id
        promise
      }
      promises.clear
      ps
    }
    ps.foreach(_.failure(cause))
  }
}
