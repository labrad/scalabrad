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
package util

import java.io.IOException
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, ExecutionException}

import Constants.{LOOKUP, MANAGER}
import data._

class LookupProvider(connection: Connection) {

  /** Maps server names to IDs. */
  private var serverCache = Map.empty[String, Long]

  /** Maps server IDs to a map from setting names to IDs. */
  private var settingCache = Map.empty[Long, Map[String, Long]]

  def clearCache {
    serverCache = Map.empty
    settingCache = Map.empty
  }
  
  def clearServer(name: String) {
    // TODO connect this to server disconnect messages from the manager
    serverCache.get(name) match {
      case Some(id) => settingCache -= id
      case None =>
    }
    serverCache -= name
  }

  /**
   * Attempt to do necessary server/setting lookups from the local cache only.
   * @param request
   */
  def doLookupsFromCache(request: NameRequest): Option[Request] = {
    val NameRequest(server, context, records) = request
    serverCache.get(server) match {
      case None => None
      case Some(id) =>
        doSettingLookupsFromCache(id, records.toList) match {
          case None => None
          case Some(records) => Some(Request(id, context, records))
        }
    }
  }

  private def doSettingLookupsFromCache(serverId: Long, records: List[NameRecord]): Option[List[Record]] = {
    def lookup(setting: String): Option[Long] =
      for (map <- settingCache.get(serverId); id <- map.get(setting)) yield id
    
    records match {
      case Nil => Some(Nil)
      case NameRecord(name, data) :: rest =>
        lookup(name) match {
          case None => None
          case Some(id) =>
            doSettingLookupsFromCache(serverId, rest) match {
              case None => None
              case Some(records) => Some(Record(id, data) :: records)
            }
        }
    }
  }

  private def doSettingLookupFromCache(serverId: Long, setting: String): Option[Long] =
    for {
      settingMap <- settingCache.get(serverId)
      id <- settingMap.get(setting)
    } yield id
  
  /**
   * Do necessary server/setting lookups, making requests to the manager as necessary.
   * @param request
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  def doLookups(request: NameRequest): Request =
    // lookup server ID
    request match {
      case NameRequest(name, context, records) =>
        val id = serverCache.get(name) match {
          case None => lookupServer(name)
          case Some(id) => id
        }
        Request(id, context, doSettingLookups(id, records.toList))
    }
  
  private def doSettingLookups(id: Long, records: List[NameRecord]): Seq[Record] = {

    var settingMap = settingCache.get(id) match {
      case Some(settingMap) => settingMap
      case None =>
        val settingMap = Map.empty[String, Long]
        settingCache += id -> settingMap
        settingMap
    }
    
    for (NameRecord(setting, data) <- records) yield {
      settingMap.get(setting) match {
        case Some(id) => Record(id, data)
        case None =>
          val request = Request(MANAGER, records = Seq(Record(LOOKUP, Cluster(Word(id), Str(setting)))))
          val response = connection.sendAndWait(request)(0)
          val Cluster(Word(serverId), Word(settingId)) = response
          settingMap += setting -> settingId
          settingCache += id -> settingMap
          Record(settingId, data)
      }
    }
  }


  /**
   * Lookup the ID of a server, pulling from the cache if we already know it.
   * The looked up ID is stored in the local cache for future use.
   * @param server
   * @return
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private def lookupServer(server: String) = {
    val request = Request(MANAGER, records = Seq(Record(LOOKUP, Str(server))))
    val response = connection.sendAndWait(request)(0)
    val Word(serverId) = response
    // cache this lookup result
    serverCache += server -> serverId
    settingCache += serverId -> Map.empty[String, Long]
    serverId
  }


  /**
   * Lookup IDs for a list of settings on the specified server.  All the setting
   * IDs are stored in the local cache for future use.
   * @param serverID
   * @param settings
   * @return
   * @throws IOException
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private def lookupSettings(serverId: Long, settings: Seq[String]) = {
    val data = Cluster(Word(serverId), Arr(settings map { Str(_) }))
    val request = Request(MANAGER, records = Seq(Record(LOOKUP, data)))
    val response = connection.sendAndWait(request)(0)
    val settingIds = response(1).getWordSeq
    // cache these lookup results
    settingCache.get(serverId) match {
      case Some(cache) =>
        settingCache += serverId -> (cache ++ (settings zip settingIds))
      case None =>
    }
    settingIds
  }
}
