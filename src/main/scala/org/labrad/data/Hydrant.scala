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
import scala.math
import scala.util.Random

import types._

object Hydrant {

  private val random = new Random

  def randomType(noneOkay: Boolean = true, listOkay: Boolean = true, depth: Int = 0): Type = {
    var min = 0
    var max = 9
    if (!noneOkay) min = 1
    if (depth >= 3) max = 7
    else if (!listOkay) max = 8
    val choice = random.nextInt(max - min + 1) + min
    choice match {
      case 0 => TEmpty
      case 1 => TBool
      case 2 => TInteger
      case 3 => TWord
      case 4 => TStr
      case 5 => TTime
      case 6 => TValue(randomUnits)
      case 7 => TComplex(randomUnits)
      case 8 => {
        val length = random.nextInt(5) + 1
        val elementTypes = for (i <- 0 until length)
          yield randomType(false, true, depth + 1)
        TCluster(elementTypes: _*)
      }
      case 9 => {
        val elementType = randomType(false, false, random.nextInt(3) + 1)
        TArr(elementType, depth + 1)
      }
      case _ =>
        throw new Exception("Invalid type choice.")
    }
  }

  def randomUnits = {
    val choices = Array(null, "", "s", "ms", "us", "m", "m/s", "V^2/Hz", "V/Hz^1/2")
    choices(random.nextInt(choices.length))
  }

  def randomData: Data = randomData(randomType())
  def randomData(s: String): Data = randomData(Type(s))

  def randomData(t: Type): Data =
    t match {
      case TEmpty => randomEmpty
      case TBool => randomBool
      case TInteger => randomInt
      case TWord => randomWord
      case TStr => randomStr
      case TTime => randomTime
      case TValue(units) => randomValue(units)
      case TComplex(units) => randomComplex(units)
      case t: TCluster => randomCluster(t)
      case t: TArr => randomList(t)
      case _ => throw new Exception("Invalid type.")
    }

  // random none
  def randomEmpty = Data.EMPTY

  // random boolean
  def randomBool = Bool(random.nextBoolean)

  // random integer
  def randomInt = Integer(random.nextInt)

  // random word
  def randomWord = Word(random.nextInt(2000000000).toLong)

  // random string
  def randomStr = {
    val bytes = Array.ofDim[Byte](random.nextInt(100))
    random.nextBytes(bytes)
    Bytes(bytes)
  }

  // random date
  def randomTime = {
    val time = System.currentTimeMillis + random.nextInt
    Time(new Date(time))
  }

  // random value
  def randomValue(units: Option[String]) = units match {
    case None => Value(nextDouble)
    case Some(units) => Value(nextDouble * 1e9, units)
  }

  private def nextDouble = random.nextGaussian * 1e9
  
  // random complex
  def randomComplex(units: Option[String]) = units match {
    case None => ComplexData(nextDouble * 1e9, nextDouble)
    case Some(units) => ComplexData(nextDouble, nextDouble, units)
  }

  def randomCluster(t: TCluster) = {
    val elements = for (i <- 0 until t.size)
      yield randomData(t(i))
    Cluster(elements: _*)
  }

  def randomList(t: TArr) = {
    val data = Data.ofType(t)
    val indices = Array.fill[Int](t.depth)(0)
    val shape = Array.tabulate[Int](t.depth) { i => random.nextInt(math.pow(2, 5 - t.depth).toInt) }
    data.setArrayShape(shape)
    fillList(data, t.elem, indices, shape)
    data
  }

  private def fillList(data: Data, elem: Type, indices: Array[Int], shape: Array[Int], depth: Int = 0) {
    if (depth == shape.length)
      data(indices: _*).set(randomData(elem))
    else
      for (i <- 0 until shape(depth)) {
        indices(depth) = i
        fillList(data, elem, indices, shape, depth + 1)
      }
  }


  /*
	def hoseDown(setting, n=1000, silent=True):
	    for _ in range(n):
	        t = randType()
	        v = randValue(t)
	        if not silent:
	            print t
	        try:
	            resp = setting(v)
	            assert v == resp
	        except:
	            print 'problem:', str(t), repr(t)
	            print str(T.flatten(v)[1]), str(T.flatten(resp)[1])
	            raise

	def hoseDataVault(dv, n=1000, silent=True):
	    for i in range(n):
	        t = randType(noneOkay=False)
	        v = randValue(t)
	        if not silent:
	            print t
	        try:
	            pname = 'p%03s' % i
	            dv.add_parameter(pname, v)
	            resp = dv.get_parameter(pname)
	            assert v == resp
	        except:
	            print 'problem:', str(t), repr(t)
	            print str(T.flatten(v)[1]), str(T.flatten(resp)[1])
	            raise
   */

}
