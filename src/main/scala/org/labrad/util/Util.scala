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

import scala.collection.JavaConversions._

object Util {
  /**
   * Converts a byte array into a hex string.
   * @param bytes
   * @return
   */
  def dumpBytes(bytes: Array[Byte]) = {
    var counter = 0
    val dump = new StringBuffer
    for (b <- bytes) {
      val high = (b & 0xF0) >> 4
      val low = (b & 0x0F)
      dump.append(HEX_DIGITS(high))
      dump.append(HEX_DIGITS(low))
      counter += 1
      if (counter == 4) {
        dump.append(" ")
        counter = 0
      }
    }
    dump.toString
  }
  
  private val HEX_DIGITS = "0123456789ABCDEF"

  /**
   * Get an environment variable, or fall back on the given default if not found.
   * @param key
   * @param defaultVal
   * @return
   */
  def getEnv(key: String, default: String) =
    System.getenv.find {
      case (k, v) => k.equalsIgnoreCase(key)
    } map {
      case (k, v) => v
    } getOrElse(default)

  /**
   * Get an integer value from the environment, returning a default if
   * the environment variable is not found or cannot be converted to an int.
   * @param key
   * @param defaultVal
   * @return
   */
  def getEnvInt(key: String, default: Int): Int =
    try {
      getEnv(key, "").toInt
    } catch {
      case e: NumberFormatException => default
    }
}
