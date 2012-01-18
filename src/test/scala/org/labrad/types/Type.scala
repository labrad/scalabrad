package org.labrad
package types

import org.scalatest.FunSuite

class TypeTests extends FunSuite {
  test("types should be parsable") {
    val tests = Seq(
      // basic types
      ("b" -> TBool),
      ("i" -> TInteger),
      ("w" -> TWord),
      ("is" -> TCluster(TInteger, TStr)),
      ("*(is)" -> TArr(TCluster(TInteger, TStr))),
      ("v" -> TValue()),
      ("v[]" -> TValue("")),
      ("v[m/s]" -> TValue("m/s")),
      ("" -> TEmpty),
      ("?" -> TAny),
      ("*?" -> TArr(TAny)),
      ("*2?" -> TArr(TAny, 2)),
      ("(s?)" -> TCluster(TStr, TAny)),
      
      // comments
      ("s: this has a trailing comment" -> TStr),
      ("ss{embedded comment}is" -> TCluster(TStr, TStr, TInteger, TStr)),
      ("ss{embedded comment}is: trailing comment" -> TCluster(TStr, TStr, TInteger, TStr)),
      
      // whitespace and commas
      ("s,s" -> TCluster(TStr, TStr)),
      ("s, s" -> TCluster(TStr, TStr)),
      ("* 3 v[m]" -> TArr(TValue("m"), 3))
    )
        
    for ((s, t) <- tests)
      assert(Type(s) == t)
  }
  
  test("matchers accept compatible types") {
    val acceptTests = Seq(
      ("" -> ""),
      ("?" -> ""),
      ("?" -> "?"),
      ("v[s]" -> "v"),
      ("v[s]" -> "v[s]"),
      ("*s" -> "*s"),
      ("*?" -> "*s"),
      ("<i|w>" -> "i"),
      ("<i|w>" -> "w"),
      ("<i|w> <s|v>" -> "is"),
      ("<i|w> <s|v>" -> "iv"),
      ("<i|w> <s|v>" -> "ws"),
      ("<i|w> <s|v>" -> "wv"),
      ("<w|s>" -> "<w|s>"),
      ("(i...)" -> "(i)"),
      ("(i...)" -> "(ii)"),
      ("(i...)" -> "(iii)"),
      ("(i...)" -> "(iiii)"),
      ("(<i|w>...)" -> "(i)"),
      ("(<i|w>...)" -> "(iw)"),
      ("(<i|w>...)" -> "(iww)"),
      ("(<i|w>...)" -> "(wwww)")
    )
    for ((t1, t2) <- acceptTests)
      assert(Type(t1) accepts Type(t2))
  }
  
  test("matchers do not accept incompatible types") {
    val notAccepted = Seq(
      ("" -> "i"),
      ("s" -> "?"),
      ("v[s]" -> "v[m]"),
      ("(ss)" -> "(si)"),
      ("*s" -> "*2s"),
      ("<i|w>" -> "s")
    )
    for ((t1, t2) <- notAccepted)
      assert(!(Type(t1) accepts Type(t2)))
  }
}
