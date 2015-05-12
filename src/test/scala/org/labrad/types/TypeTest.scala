package org.labrad.types

import org.scalatest.FunSuite

class TypeTests extends FunSuite {
  test("patterns should be parsable") {
    val tests: Seq[(String, Pattern)] = Seq(
      // basic types
      "" -> TNone,
      "_" -> TNone,
      "b" -> TBool,
      "i" -> TInt,
      "w" -> TUInt,
      "is" -> PCluster(TInt, TStr),
      "*(is)" -> PArr(PCluster(TInt, TStr)),
      "v" -> PValue(),
      "v[]" -> PValue(""),
      "v[m/s]" -> PValue("m/s"),
      "c" -> PComplex(),
      "c[]" -> PComplex(""),
      "c[m/s]" -> PComplex("m/s"),
      "?" -> PAny,
      "*?" -> PArr(PAny),
      "*2?" -> PArr(PAny, 2),
      "s?" -> PCluster(TStr, PAny),
      "(s?)" -> PCluster(TStr, PAny)
    )

    for ((s, p) <- tests) {
      assert(Pattern(s) == p)
    }
  }

  test("types should be parsable") {
    val tests = Seq(
      // basic types
      "" -> TNone,
      "_" -> TNone,
      "b" -> TBool,
      "i" -> TInt,
      "w" -> TUInt,
      "is" -> TCluster(TInt, TStr),
      "*(is)" -> TArr(TCluster(TInt, TStr)),
      "v" -> TValue(),
      "v[]" -> TValue(""),
      "v[m/s]" -> TValue("m/s"),
      "c" -> TComplex(),
      "c[]" -> TComplex(""),
      "c[m/s]" -> TComplex("m/s")
    )

    for ((s, t) <- tests)
      assert(Type(s) == t)
  }

  test("types can contain whitespace") {
    val tests = Seq(
      " b" -> TBool,
      "b " -> TBool,
      " b b \t b " -> TCluster(TBool, TBool, TBool),
      " * 3 v [m] " -> TArr(TValue("m"), 3)
    )

    for ((s, t) <- tests)
      assert(Type(s) == t)
  }

  test("cluster elements can be separated by commas") {
    val tests = Seq(
      "s,s" -> TCluster(TStr, TStr),
      "(s,s)" -> TCluster(TStr, TStr)
    )
    for ((s, t) <- tests)
      assert(Type(s) == t)
  }

  test("types can have a trailing comment") {
    val tests = Seq(
      // comments
      ": this has a trailing comment" -> TNone,
      " : this has a trailing comment" -> TNone,
      "_: this has a trailing comment" -> TNone,
      "s: this has a trailing comment" -> TStr
    )

    for ((s, t) <- tests)
      assert(Type(s) == t)
  }

  test("types can have embedded comments") {
    val tests = Seq(
      // comments
      "ss{embedded comment}is" -> TCluster(TStr, TStr, TInt, TStr),
      "{embedded comment}ssis" -> TCluster(TStr, TStr, TInt, TStr)
    )

    for ((s, t) <- tests)
      assert(Type(s) == t)
  }

  test("embedded comments can contain a colon") {
    val tests = Seq(
      // comments
      "ss{embedded comment: this is a test}is" -> TCluster(TStr, TStr, TInt, TStr),
      "{embedded comment: this, too}ssis" -> TCluster(TStr, TStr, TInt, TStr)
    )

    for ((s, t) <- tests)
      assert(Type(s) == t)
  }

  test("top-level pattern alternatives can be separated by |") {
    assert(Pattern("s|t") == PChoice(TStr, TTime))

    // handle empties
    assert(Pattern("s|_") == PChoice(TStr, TNone))
    assert(Pattern("_|s") == PChoice(TNone, TStr))
    intercept[Exception] { Pattern("|s") }
    intercept[Exception] { Pattern("s|") }
  }

  test("matchers accept compatible types") {
    val acceptTests = Seq(
      "" -> "",
      "?" -> "",
      "?" -> "?",
      "v[s]" -> "v[s]",
      "*s" -> "*s",
      "*?" -> "*s",
      "<i|w>" -> "i",
      "<i|w>" -> "w",
      "<i|w> <s|v>" -> "is",
      "<i|w> <s|v>" -> "iv",
      "<i|w> <s|v>" -> "ws",
      "<i|w> <s|v>" -> "wv",
      "<w|s>" -> "<w|s>",
      "(i...)" -> "(i)",
      "(i...)" -> "(ii)",
      "(i...)" -> "(iii)",
      "(i...)" -> "(iiii)",
      "(<i|w>...)" -> "(i)",
      "(<i|w>...)" -> "(iw)",
      "(<i|w>...)" -> "(iww)",
      "(<i|w>...)" -> "(wwww)",
      "(<i|w>...)" -> "(wiwwi)",
      "<i|w|s>" -> "<i|w>"
    )
    for ((t1, t2) <- acceptTests) {
      assert(Pattern(t1) accepts Pattern(t2), s"$t1 should have accepted $t2")
    }
  }

  test("matchers do not accept incompatible types") {
    val notAccepted = Seq(
      "" -> "i",
      "s" -> "?",
      "v[s]" -> "v[m]",
      "(ss)" -> "(si)",
      "*s" -> "*2s",
      "<i|w>" -> "s",
      "<i|w>" -> "<i|s>"
    )
    for ((t1, t2) <- notAccepted)
      assert(!(Pattern(t1) accepts Pattern(t2)))
  }
}
