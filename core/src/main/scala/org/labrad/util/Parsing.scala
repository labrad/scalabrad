package org.labrad.util

import fastparse._
//import fastparse.internals.IndexedParserInput
//import fastparse.parsers.Intrinsics

object Parsing {

  //implicit val stringReprOps = fastparse.StringReprOps

  /**
   * A fastparse parser defined by a regular expression.
   */
  def Re(pattern: String)(implicit ctx: P[_]): P[Unit] = {
    val regex = pattern.r
    val input = ctx.input
    val rest = input.slice(ctx.index, input.length)
    regex.findPrefixMatchOf(rest) match {
      case None => ctx.freshFailure()
      case Some(result) => ctx.freshSuccessUnit(ctx.index + result.end)
    }
  }

  /**
   * Parse a string with the given parser and return the value or raise an exception.
   */
  def parseOrThrow[A](p: P[_] => P[A], s: String): A =
    parse(s, p) match {
      case Parsed.Success(d, _) => d
      case failure: Parsed.Failure => sys.error(failure.msg)
    }

  /**
   * Parser for the given token literal.
   */
  def Token[T: P](token: String): P[Unit] = P(token)

  /**
   * Parser for whitespace that uses the java regex character class.
   */
  def Whitespace[T: P] = P( Re("""\s*""") )
}
