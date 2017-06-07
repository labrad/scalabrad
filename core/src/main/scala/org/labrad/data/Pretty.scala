package org.labrad.data

/**
 * An implementation of pretty printing based on "A Prettier Printer" by Philip Wadler:
 * https://homepages.inf.ed.ac.uk/wadler/papers/prettier/prettier.pdf
 */
object Pretty {
  /**
   * A "non-deterministic" document, which is a collection of documents with different layouts but
   * the same flattened form. The Union combinator presents multiple options for a given doc (or
   * part of a doc). To pretty-print something, one must produce a Doc and then call the pretty
   * method and pass in the desired width into which the output should fit.
   *
   * Corresponds to the DOC datatype in the original paper.
   */
  sealed trait Doc { x =>
    import Doc._

    def <> (y: Doc): Doc = Concat(x, y)
    def <|> (y: => Doc): Doc = Union(x, y)
    def <+> (y: Doc): Doc = x <> text(" ") <> y
    def </> (y: Doc): Doc = x <> line <> y
    def <+/> (y: Doc): Doc = x <> (text(" ") <|> line) <> y

    /**
     * To pretty-print a doc for a given width, we first choose the best SimpleDoc representation
     * that fits within the given width and then lay it out.
     */
    def pretty(w: Int): String = best(w, 0, x).layout
  }

  object Doc {
    case object Empty extends Doc
    case object Line extends Doc
    case class Text(text: String) extends Doc
    case class Nest(indent: Int, doc: Doc) extends Doc
    case class Concat(left: Doc, right: Doc) extends Doc
    case class Union(left: Doc, right: Doc) extends Doc {
      override def <> (other: Doc): Doc = (left <> other) <|> (right <> other)
    }
  }

  /**
   * A basic representation of a single document with no unions, and hence no non-determinism.
   * There is only one way to layout a SimpleDoc.
   *
   * Corresponds to the Doc datatype in the original paper.
   */
  private sealed trait SimpleDoc {
    import SimpleDoc._

    def layout: String = {
      val b = new StringBuilder
      buildLayout(b)
      b.toString
    }

    private def buildLayout(b: StringBuilder): Unit = this match {
      case Empty =>
      case Text(s, x) =>
        b ++= s
        x.buildLayout(b)
      case Line(i, x) =>
        b ++= "\n" + (" " * i)
        x.buildLayout(b)
    }
  }
  private object SimpleDoc {
    case object Empty extends SimpleDoc
    case class Text(s: String, doc: SimpleDoc) extends SimpleDoc
    case class Line(i: Int, doc: SimpleDoc) extends SimpleDoc
  }

  // Find the best SimpleDoc representation of a Doc.
  // The implementation uses Stream which is like List except that the tail is lazily-evaluated
  // (like Haskell lists). Note that #:: is the Stream concatenation operator (like :: for List).
  private def best(w: Int, k: Int, x: Doc): SimpleDoc = be(w, k, Stream((0, x)))

  private def be(w: Int, k: Int, xs: Stream[(Int, Doc)]): SimpleDoc = xs match {
    case Stream.Empty                => SimpleDoc.Empty
    case (i, Doc.Empty)        #:: z => be(w, k, z)
    case (i, Doc.Line)         #:: z => SimpleDoc.Line(i, be(w, i, z))
    case (i, Doc.Text(s))      #:: z => SimpleDoc.Text(s, be(w, k + s.length, z))
    case (i, Doc.Nest(j, x))   #:: z => be(w, k, (i+j, x) #:: z)
    case (i, Doc.Concat(x, y)) #:: z => be(w, k, (i, x) #:: (i, y) #:: z)
    case (i, Doc.Union(x, y))  #:: z => better(w, k, be(w, k, (i, x) #:: z), be(w, k, (i, y) #:: z))
  }

  private def better(w: Int, k: Int, x: SimpleDoc, y: => SimpleDoc): SimpleDoc = {
    if (fits(w - k, x)) x else y
  }

  private def fits(w: Int, x: SimpleDoc): Boolean = {
    if (w < 0) {
      false
    } else {
      x match {
        case SimpleDoc.Empty      => true
        case SimpleDoc.Text(s, x) => fits(w - s.length, x)
        case SimpleDoc.Line(i, x) => true
      }
    }
  }

  // Functions for constructing docs.
  def empty: Doc = Doc.Empty
  def text(s: String): Doc = Doc.Text(s)
  def line: Doc = Doc.Line
  def nest(k: Int, doc: Doc): Doc = doc match {
    case Doc.Union(x, y) => Doc.Nest(k, x) <|> Doc.Nest(k, y)
    case doc => Doc.Nest(k, doc)
  }

  // Create a group that can be layed out as-is or flattened onto one line.
  def group(x: Doc): Doc = flatten(x) <|> x

  private def flatten(x: Doc): Doc = x match {
    case Doc.Empty => Doc.Empty
    case Doc.Concat(x, y) => flatten(x) <> flatten(y)
    case Doc.Nest(i, x) => Doc.Nest(i, flatten(x))
    case Doc.Text(s) => Doc.Text(s)
    case Doc.Line => Doc.Text("")
    case Doc.Union(x, y) => flatten(x)
  }


  def folddoc(f: (Doc, Doc) => Doc, xs: Stream[Doc]): Doc = xs.fold(empty)(f)
  def spread(xs: Stream[Doc]): Doc = folddoc(_ <+> _, xs)
  def stack(xs: Stream[Doc]): Doc = folddoc(_ </> _, xs)

  // A block enclosed by left and right delimiters, with the body on one line or else indented.
  def bracket(l: String, x: Doc, r: String): Doc =
    group(text(l) <> nest(4, x) <> line <> text(r))

  // Similar to bracket, but takes a stream of Docs with a delimiter to separate them.
  def bracketAll(l: String, xs: Stream[Doc], r: String, delim: String = ", "): Doc =
    bracket(l, stack(sep(xs, delim)), r)

  // Add delimiter test to separate a stream of Docs, with no trailing delimiter.
  def sep(xs: Stream[Doc], delim: String): Stream[Doc] = xs match {
    case Stream.Empty       => Stream.Empty
    case x #:: Stream.Empty => x #:: Stream.Empty
    case x #:: xs           => (x <> text(delim)) #:: sep(xs, delim)
  }
}
