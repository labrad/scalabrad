package org.labrad.manager

import scala.collection.mutable

trait StatsTracker {
  def connectClient(id: Long, name: String): Unit
  def connectServer(id: Long, name: String): Unit
  def disconnect(id: Long): Unit
  def serverReq(id: Long): Unit
  def serverRep(id: Long): Unit
  def clientReq(id: Long): Unit
  def clientRep(id: Long): Unit
  def msgSend(id: Long): Unit
  def msgRecv(id: Long): Unit
  def stats: Seq[ConnectionStats]
}

class StatsTrackerImpl extends StatsTracker {
  class ConnStatsMutable(
    id: Long,
    name: String,
    isServer: Boolean,
    var sReqs: Long = 0,
    var sReps: Long = 0,
    var cReqs: Long = 0,
    var cReps: Long = 0,
    var msgSent: Long = 0,
    var msgRecd: Long = 0
  ) {
    def immutable = ConnectionStats(id, name, isServer, sReqs, sReps, cReqs, cReps, msgSent, msgRecd)
  }

  private val statsMap = mutable.Map.empty[Long, ConnStatsMutable]
  private def update(id: Long, f: ConnStatsMutable => Unit): Unit = synchronized { statsMap.get(id).map(f) }

  def connectClient(id: Long, name: String): Unit = synchronized { statsMap(id) = new ConnStatsMutable(id, name, isServer = false) }
  def connectServer(id: Long, name: String): Unit = synchronized { statsMap(id) = new ConnStatsMutable(id, name, isServer = true) }
  def disconnect(id: Long): Unit = synchronized { statsMap -= id }

  def serverReq(id: Long): Unit = update(id, _.sReqs += 1)
  def serverRep(id: Long): Unit = update(id, _.sReps += 1)
  def clientReq(id: Long): Unit = update(id, _.cReqs += 1)
  def clientRep(id: Long): Unit = update(id, _.cReps += 1)
  def msgSend(id: Long): Unit = update(id, _.msgSent += 1)
  def msgRecv(id: Long): Unit = update(id, _.msgRecd += 1)

  def stats: Seq[ConnectionStats] = {
    synchronized { statsMap.values.map(_.immutable).toVector }.sortBy(_.id)
  }
}

case class ConnectionStats(
  id: Long,
  name: String,
  isServer: Boolean,
  sReqs: Long,
  sReps: Long,
  cReqs: Long,
  cReps: Long,
  msgSent: Long,
  msgRecd: Long
)
