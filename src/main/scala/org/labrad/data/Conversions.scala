package org.labrad
package data

object Conversions {
  implicit def tuple2ToData(t: (Data, Data)): Data = Cluster(t._1, t._2)
  implicit def tuple3ToData(t: (Data, Data, Data)): Data = Cluster(t._1, t._2, t._3)
  implicit def tuple4ToData(t: (Data, Data, Data, Data)): Data = Cluster(t._1, t._2, t._3, t._4)
}