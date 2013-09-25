package org.labrad.data

import org.labrad.types._

/**
 * A utility class that encapsulates an array of complex numbers.
 *
 * This class is much more efficient than having an array of Complex objects
 * because only two arrays need to be allocated, not one object per element
 * in the array.  In addition, this class has convenience methods for converting
 * to and from LabRAD data.
 */
case class ComplexArray(re: Array[Double], im: Array[Double], units: Option[String] = None) {
  require(re.length == im.length)
  def length = re.length
  def size = re.length

  def apply(i: Int) = Complex(re(i), im(i))

  /** Convert a complex array into LabRAD data of type *c */
  def toData: Data = {
    val iq = Data(TArr(TComplex(units), 1))
    iq.setArraySize(length)
    for (i <- 0 until length)
      iq(i).setComplex(re(i), im(i))
    iq
  }
}

object ComplexArray {
  /**
   * Create a complex array from LabRAD data of type *c
   * @param vals
   * @return
   */
  def apply(vals: Data): ComplexArray = vals.t match {
    case TArr(TComplex(_), 1) =>
      val len = vals.arraySize
      val re = Array.ofDim[Double](len)
      val im = Array.ofDim[Double](len)
      for (i <- 0 until len) {
        val c = vals(i).getComplex
        re(i) = c.real
        im(i) = c.imag
      }
      ComplexArray(re, im)
    case t =>
      sys.error(s"cannot convert type $t to *c")
  }
}
