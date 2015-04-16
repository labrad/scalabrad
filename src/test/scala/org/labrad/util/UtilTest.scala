package org.labrad.util

import org.scalatest.{FunSuite, Matchers}

class UtilTest extends FunSuite with Matchers {

  test("parseArg accepts allowed arguments") {
    val parsed = Util.parseArgs(Array("--foo=x"), Seq("foo"))
    assert(parsed.get("foo") == Some("x"))
  }

  test("parseArg allows arguments to be empty") {
    val parsed = Util.parseArgs(Array("--foo="), Seq("foo"))
    assert(parsed.get("foo") == Some(""))
  }

  test("parseArg allows arguments to be missing") {
    val parsed = Util.parseArgs(Array("--foo=x"), Seq("foo", "bar"))
    assert(parsed.get("foo") == Some("x"))
    assert(parsed.get("bar") == None)
  }

  test("parseArg fails on unknown arguments") {
    an[Exception] should be thrownBy {
      Util.parseArgs(Array("--baz=x"), Seq("foo", "bar"))
    }
  }
}
