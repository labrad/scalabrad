package org.labrad
package types

import org.scalatest.FunSuite

class TypeSpec extends FunSuite {
  test("matchers accept compatible types") {
    val acceptTests = Seq(
      ("", ""),
      ("?", ""),
      ("?", "?"),
      ("v[s]", "v"),
      ("v[s]", "v[s]"),
      ("*s", "*s"),
      ("*?", "*s"),
      ("<i|w>", "i"),
      ("<i|w>", "w"),
      ("<i|w> <s|v>", "is"),
      ("<i|w> <s|v>", "iv"),
      ("<i|w> <s|v>", "ws"),
      ("<i|w> <s|v>", "wv"),
      ("<w|s>", "<w|s>"),
      ("(i...)", "(i)"),
      ("(i...)", "(ii)"),
      ("(i...)", "(iii)"),
      ("(i...)", "(iiii)"),
      ("(<i|w>...)", "(i)"),
      ("(<i|w>...)", "(iw)"),
      ("(<i|w>...)", "(iww)"),
      ("(<i|w>...)", "(wwww)")
    )
    for ((t1, t2) <- acceptTests)
      assert(Type(t1) accepts Type(t2))
  }
  
  test("matchers do not accept incompatible types") {
    val notAccepted = Seq(
      ("", "i"),
      ("s", "?"),
      ("v[s]", "v[m]"),
      ("(ss)", "(si)"),
      ("*s", "*2s"),
      ("<i|w>", "s")
    )
    for ((t1, t2) <- notAccepted)
      assert(!(Type(t1) accepts Type(t2)))
  }
}
