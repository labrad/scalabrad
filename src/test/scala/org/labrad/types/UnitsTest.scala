package org.labrad.types

import org.scalatest.FunSuite

class RatioTests extends FunSuite {
  test("ratios are always in lowest terms") {
    val r = Ratio(3, 6)
    assert(r.num == 1)
    assert(r.denom == 2)
  }
  
  test("ratios cannot have 0 denominator") {
    try {
      val r = Ratio(2, 0)
      sys.error("ratio created with 0 denominator")
    } catch {
      case _: IllegalArgumentException => // expected
    }
  }
  
  test("ratios always have positive denominators") {
    val r1 = Ratio(1, -2)
    assert(r1.num == -1)
    assert(r1.denom == 2)
    
    val r2 = Ratio(-1, -2)
    assert(r2.num == 1)
    assert(r2.denom == 2)
  }
  
  test("ratios can be multiplied") {
    val r = Ratio(1, 2) * Ratio(6, 7)
    assert(r.num == 3)
    assert(r.denom == 7)
    
    val r2 = Ratio(1, 2) * 5
    assert(r2.num == 5)
    assert(r2.denom == 2)
  }
  
  test("ratios can be divided") {
    // TODO
  }
  
  test("ratios can be added") {
    // TODO
  }
  
  test("ratios can be subtracted") {
    // TODO
  }
  
  test("ratios can be tested for equality") {
    assert(Ratio(1, 2) == Ratio(2, 4))
    assert(Ratio(0) == Ratio(0, 6))
    assert(Ratio(2) == 2)
    assert(Ratio(0) == 0)
    assert(Ratio(-1) == -1)
    
    assert(Ratio(1, 2) != Ratio(1, 3))
    assert(Ratio(0) != Ratio(1, 6))
    assert(Ratio(0) != 1)
    assert(Ratio(1) != 0)
  }
}

class UnitTests extends FunSuite {
  test("basic units can be parsed") {
    val tests = Seq(
        "m",
        "s",
        "m/s",
        "/s",
        "m*s",
        "m^2",
        "m^1/2",
        "m^-2",
        "m^-1/2"
    )
    for (test <- tests)
      assert(Units.parse(test) != null)
  }
  
  def approxEq(a: Double, b: Double) = math.abs(a - b) < 10e-8
  def converts(a: Double, ua: String, b: Double, ub: String) = {
    approxEq(Units.convert(ua, ub)(a), b)
  }
  
  test("nonlinear units") {
    assert(Units.parse("dBm") != null)
    assert(converts(0, "dBm", 1, "mW"))
    assert(converts(0, "dBW", 1, "W"))
    assert(converts(0, "degC", 273.15, "K"))
  }
  
  test("nonlinear units can be used as factors") {
    assert(converts(1, "dBm/s", 60, "dBm/min"))
    assert(converts(1, "dBm^2", 1, "dBm*dBm"))
  }
}
