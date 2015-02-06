package org.labrad.util

import org.labrad.Connection
import org.labrad.data._
import org.labrad.manager.Manager
import scala.concurrent.{ExecutionContext, Future}

class LookupProvider(send: Request => Future[Seq[Data]]) {

  private var serverCache = Map.empty[String, Long] // server name -> server id
  private var settingCache = Map.empty[(String, String), Long] // (server name, setting name) -> setting id

  def clearCache: Unit = {
    serverCache = Map.empty
    settingCache = Map.empty
  }

  def clearServer(name: String): Unit = {
    // TODO connect this to server disconnect messages from the manager
    settingCache = settingCache.filterKeys { case (server, _) => server != name }
    serverCache -= name
  }

  def resolve(request: NameRequest)(implicit ex: ExecutionContext): Future[Request] =
    lookupLocal(request) match {
      case Some(request) => Future.successful(request)
      case None => lookupRemote(request)
    }

  private def lookupLocal(request: NameRequest): Option[Request] = {
    val NameRequest(server, context, records) = request

    def lookupRecord(r: NameRecord): Option[Record] =
      settingCache.get((server, r.name)).map(id => Record(id, r.data))

    def allOrNone[T](opts: Seq[Option[T]]): Option[Seq[T]] =
      if (opts.forall(_.isDefined)) Some(opts.map(_.get)) else None

    for {
      id <- serverCache.get(server)
      recs <- allOrNone(records.map(lookupRecord))
    } yield Request(id, context, recs)
  }

  private def lookupRemote(request: NameRequest)(implicit ec: ExecutionContext): Future[Request] = {
    val NameRequest(server, context, records) = request
    for {
      id <- lookupServer(server)
      recs <- Future.sequence {
        records.map(r => lookupSetting(server, r.name).map(id => Record(id, r.data)))
      }
    } yield Request(id, context, recs)
  }

  private def lookupServer(server: String)(implicit ec: ExecutionContext): Future[Long] =
    serverCache.get(server) match {
      case Some(id) => Future.successful(id)
      case None => doLookup(Str(server)) map {
        case UInt(id) =>
          serverCache += server -> id
          id
      }
    }

  private def lookupSetting(server: String, setting: String)(implicit ec: ExecutionContext): Future[Long] =
    settingCache.get((server, setting)) match {
      case Some(id) => Future.successful(id)
      case None => doLookup(Cluster(Str(server), Str(setting))) map {
        case Cluster(_, UInt(id)) =>
          settingCache += (server, setting) -> id
          id
      }
    }

  private def doLookup(data: Data)(implicit ec: ExecutionContext): Future[Data] =
    send(Request(Manager.ID, records = Seq(Record(Manager.LOOKUP, data)))).map(_(0))
}
