package org.labrad.macros

import org.labrad.data._
import org.scalatest.FunSuite
import scala.concurrent.Await
import scala.concurrent.duration._

/*
class MacroTestServer {
  def foo(a: Int, b: String = "default"): (String, String) = {
    (s"int: $a", s"str: $b")
  }
}

class MacrosTest extends FunSuite {

  import scala.concurrent.ExecutionContext.Implicits.global

  val s = new MacroTestServer
  val handlers = Macros.routes("""
    CALL foo s.foo
  """)

  test("call simple route") {
    val f = handlers("foo").call((1, "a").toData)
    val result = Await.result(f, 1.second)
    assert(result.get[(String, String)] == ("int: 1", "str: a"))
  }

  test("call simple route with default value") {
    val f = handlers("foo").call(Tuple1(1).toData)
    val result = Await.result(f, 1.second)
    assert(s.foo(1) == ("int: 1", "str: default"))
    assert(result.get[(String, String)] == ("int: 1", "str: default"))
  }

}
*/