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

package org.labrad.data

import java.util.Date

import org.labrad.types._

trait Setter[T] {
  // the LabRAD type that this setter sets.
  def typ: Type

  // set a Data object with a value of the correct type.
  def set(data: Data, value: T): Unit
}

object Setters {
  val boolSetter = new Setter[Boolean] {
    val typ = TBool
    def set(data: Data, value: Boolean) { data.setBool(value) }
  }
  val intSetter = new Setter[Int] {
    val typ = TInteger
    def set(data: Data, value: Int) { data.setInt(value) }
  }
  val wordSetter = new Setter[Long] {
    val typ = TWord
    def set(data: Data, value: Long) { data.setWord(value) }
  }
  val stringSetter = new Setter[String] {
    val typ = TStr
    def set(data: Data, value: String) { data.setString(value) }
  }
  val dateSetter = new Setter[Date] {
    val typ = TTime
    def set(data: Data, value: Date) { data.setTime(value) }
  }
  val valueSetter = new Setter[Double] {
    val typ = TValue()
    def set(data: Data, value: Double) { data.setValue(value) }
  }
  val complexSetter = new Setter[Complex] {
    val typ = TComplex()
    def set(data: Data, value: Complex) { data.setComplex(value) }
  }
}
