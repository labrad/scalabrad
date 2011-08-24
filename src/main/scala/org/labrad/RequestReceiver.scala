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

import java.util.concurrent.{CancellationException, ExecutionException, Future, TimeUnit, TimeoutException}

import javax.swing.SwingUtilities.invokeLater

import data.{Data, Packet, Record, Request}
import errors.LabradException

/**
 * Represents a pending LabRAD request.
 */
class RequestReceiver(request: Request, callback: Option[RequestCallback] = None) extends Future[Seq[Data]] {

  /** Status of this request. */
  private object Status extends Enumeration {
    val PENDING, DONE, FAILED, CANCELLED = Value
  }
  import Status._

  private var status = PENDING
  private var response: Seq[Data] = _
  private var cause: Throwable = _

  /**
   * Cancel this request.  If mayInterruptIfRunning is true, this
   * will immediately interrupt any threads waiting on this object,
   * notifying them of the cancellation.  Note that this does not
   * actually send any cancellation information to the server against
   * whom the request was made.
   * @param mayInterruptIfRunning a boolean indicating whether
   * to interrupt threads that are waiting to get the result
   * @return true if the request was canceled
   */
  def cancel(mayInterruptIfRunning: Boolean): Boolean = synchronized {
    status match {
      case PENDING =>
        status = CANCELLED
        cause = new CancellationException
        if (mayInterruptIfRunning) notifyAll
        true
      case _ =>
        false
    }
  }

  /**
   * Wait for the request to complete and get the result.
   * @return the Data received in response to the original request
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws ExecutionException if an error occurred while making the request
   */
  def get = synchronized {
    while (!isDone) wait
    status match {
      case CANCELLED => throw new CancellationException
      case FAILED => throw new ExecutionException(cause)
      case _ =>
    }
    response
  }

  /**
   * Wait for the specified amount of time for the request to complete.
   * @param duration the amount of time to wait
   * @param timeUnit the unit interval of time in which duration is specified
   * @return the Data received in response to the original request
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws TimeoutException if the request did not complete in the specified time
   * @throws CancellationException if the request was cancelled in the specified time
   * @throws ExecutionException if an error occurred while making the request
   */
  def get(duration: Long, timeUnit: TimeUnit) = synchronized {
    while (!isDone) wait(TimeUnit.MILLISECONDS.convert(duration, timeUnit))
    status match {
      case PENDING => throw new TimeoutException
      case CANCELLED => throw new CancellationException
      case FAILED => throw new ExecutionException(cause)
      case DONE => response
    }
  }

  /**
   * Returns true if this request was cancelled before it completed normally.
   * @return true if the request was cancelled before it completed
   */
  def isCancelled = synchronized { status == CANCELLED }

  /**
   * Returns true if this request is completed, due either to cancellation, normal
   * termination, or an ExecutionException
   * @return true if the request is completed
   */
  def isDone = synchronized { status != PENDING }

  /**
   * Set the result of this request.  Depending on whether the response
   * packet contains error records, this may result in either successful
   * completion, or in failure and an ExecutionException being created.
   * @param packet the LabRAD response packet received for this request
   */
  def set(packet: Packet) = synchronized {
    if (!isCancelled) {
      packet.records.find(_.data.isError) match {
        case Some(rec) =>
          status = FAILED
          cause = new LabradException(rec.data)
          callbackFailure
        case None =>
          status = DONE
          response = packet.records.map(_.data)
          callbackSuccess
      }
    }
    notifyAll
    ()
  }

  /**
   * Record the failure of this request.
   * @param theCause
   */
  def fail(theCause: Throwable) = synchronized {
    status = FAILED
    cause = theCause
    callbackFailure
    notifyAll
    ()
  }

  /**
   * Trigger the onSuccess callback of this request.
   */
  private def callbackSuccess {
    doLater { _.onSuccess(request, response) }
  }

  /**
   * Trigger the onFailure callback of this request.
   */
  private def callbackFailure {
    doLater { _.onFailure(request, cause) }
  }

  /**
   * Dispatch a callback by scheduling it to be invoked later.
   * We use the standard SwingUtilities.invokeLater mechanism to do this.
   * @param runnable the object to schedule for running by the event loop
   */
  private def doLater(func: RequestCallback => Unit) {
    callback.map(cb => invokeLater(new Runnable { def run = func(cb) }))
  }
}
