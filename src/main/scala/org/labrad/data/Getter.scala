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
package data

import java.util.Date

import types._

object Getters {
  // primitive getters
  val boolGetter: Data => Boolean = _.getBool
  val intGetter: Data => Int = _.getInt
  val wordGetter: Data => Long = _.getWord
  val stringGetter: Data => String = _.getString
  val dateGetter: Data => Date = _.getTime
  val valueGetter: Data => Double = _.getValue
  val complexGetter: Data => Complex = _.getComplex
  val dataGetter: Data => Data = data => data

  // Seq getters
  val boolSeqGetter: Data => Seq[Boolean] = _.getBoolSeq
  val intSeqGetter: Data => Seq[Int] = _.getIntSeq
  val wordSeqGetter: Data => Seq[Long] = _.getWordSeq
  val stringSeqGetter: Data => Seq[String] = _.getStringSeq
  val dataSeqGetter: Data => Seq[Data] = _.getDataSeq

  // array getters
  val byteArrayGetter: Data => Array[Byte] = _.getBytes
  val boolArrayGetter: Data => Array[Boolean] = _.getBoolArray
  val intArrayGetter: Data => Array[Int] = _.getIntArray
  val wordArrayGetter: Data => Array[Long] = _.getWordArray
  val valueArrayGetter: Data => Array[Double] = _.getValueArray
  val stringArrayGetter: Data => Array[String] = _.getStringArray
  val dataArrayGetter: Data => Array[Data] = _.getDataArray
}
