package org.labrad.util

import fastparse.WhitespaceApi
import fastparse.core
import fastparse.parsers.Intrinsics

object Parsing {
  /**
   * A fastparse parser defined by a regular expression.
   */
  case class Re(pattern: String) extends core.Parser[Unit] {
    val regex = pattern.r

    def parseRec(cfg: core.ParseCtx, index: Int): core.Mutable[Unit] = {
      val input = cfg.input
      val rest = input.subSequence(index, input.length)
      regex.findPrefixMatchOf(rest) match {
        case None =>
          fail(cfg.failure, index)

        case Some(result) =>
          success(cfg.success, (), index + result.end, Nil, false)
      }
    }
  }

  /**
   * Parse a string with the given parser and return the value or raise an exception.
   */
  def parseOrThrow[A](p: core.Parser[A], s: String): A =
    p.parse(s) match {
      case core.Parsed.Success(d, _) => d
      case failure: core.Parsed.Failure => sys.error(failure.msg)
    }

  /**
   * Parser for the given token literal.
   */
  def Token(token: String): core.Parser[Unit] = Intrinsics.StringIn(token)

  /**
   * Parser for whitespace that uses the java regex character class.
   */
  val Whitespace = Re("""\s*""")

  /**
   * Whitespace-insensitive version of fastparse parsers.
   * To use this, add the following two imports:
   *
   * import fastparse.noApi._
   * import Parsing.AllowWhitespace._
   *
   * If you also want to parse leading and trailing whitespace, you
   * can make a parser like:
   *
   * P( Parsing.Whitespace ~ myParser ~ Parsing.Whitespace ~ End )
   */
  val AllowWhitespace = WhitespaceApi.Wrapper {
    import fastparse.all._
    NoTrace(Whitespace)
  }
}
