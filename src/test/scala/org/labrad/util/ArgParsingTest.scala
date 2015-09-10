package org.labrad.util

import org.scalatest._

class ArgParsingTest extends FunSuite {

  def check(args: Array[String], expected: Array[String]): Unit = {
    val result = ArgParsing.expandLongArgs(args)
    assert(result.toSeq == expected.toSeq)
  }

  test("conventional long args are passed through unchanged") {
    check(Array("--foo", "bar"),
          Array("--foo", "bar"))
  }

  test("GNU-style long args are expanded") {
    check(Array("--foo=bar"),
          Array("--foo", "bar"))
  }

  test("long args can contain hyphen") {
    check(Array("--foo-bar=baz"),
          Array("--foo-bar", "baz"))
  }

  test("long args can contain underscore") {
    check(Array("--foo_bar=baz"),
          Array("--foo_bar", "baz"))
  }

  test("values can contain equals sign") {
    check(Array("--foo=bar?q=baz"),
          Array("--foo", "bar?q=baz"))
  }
}
