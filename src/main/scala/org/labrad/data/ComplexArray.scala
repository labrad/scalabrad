package org.labrad
package data

/**
 * A utility class that encapsulates an array of complex numbers.
 * 
 * This class is much more efficient than having an array of Complex objects
 * because only two arrays need to be allocated, not one object per element
 * in the array.  In addition, this class has convenience methods for converting
 * to and from LabRAD data.
 */
class ComplexArray(val re: Array[Double], val im: Array[Double]) {
  require(re.length == im.length)
  val length = re.length

  /**
   * Convert a complex array into LabRAD data of type *c
   */
  def toData: Data = {
    val iq = Data.ofType("*c")
    iq.setArraySize(length)
    for (i <- 0 until length)
      iq.setComplex(re(i), im(i), i)
    iq
  }
}

object ComplexArray {
  def apply(re: Array[Double], im: Array[Double]): ComplexArray = new ComplexArray(re, im)
    
  /**
   * Create a complex array from LabRAD data of type *c
   * @param vals
   * @return
   */
  def apply(vals: Data): ComplexArray = {
    val len = vals.getArraySize
    val re = Array.ofDim[Double](len)
    val im = Array.ofDim[Double](len)
    for (i <- 0 until len) {
      val c = vals(i).getComplex
      re(i) = c.real
      im(i) = c.imag
    }
    ComplexArray(re, im)
  }
  
  // pattern match to extract real and imaginary parts of data
  def unapply(a: ComplexArray): Option[(Array[Double], Array[Double])] = Some((a.re, a.im))
}
