package org.labrad.macros

import java.util.Date
import org.labrad.{RequestContext, SettingInfo, TypeInfo}
import org.labrad.annotations.Setting
import org.labrad.data.{Data, DataBuilder, Error, Getter, ToData}
import org.labrad.types.{Pattern, PArr, PChoice, PCluster, PValue, TNone}
import scala.concurrent.{ExecutionContext, Future}
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.macros.blackbox.Context

/**
 * An object that can handle labrad calls.
 */
trait Handler2[A] {
  def call(instance: A, request: RequestContext)(implicit ec: ExecutionContext): Future[Data]

  def settingInfo: SettingInfo
  def accepts = settingInfo.accepts.pat
  def returns = settingInfo.returns.pat
}

trait SimpleHandler[A] {
  def accepts: Pattern
  def returns: Pattern

  def call(instance: A, request: RequestContext)(implicit ec: ExecutionContext): Future[Data]

  def checkRequest(request: RequestContext): Unit = {
    require(accepts.accepts(request.data.t))
  }
}

/** Labrad handler for methods that return a future. */
class OverloadedHandler[A](
  id: Long,
  name: String,
  doc: String,
  overloads: Seq[SimpleHandler[A]]
) extends Handler2[A] {
  val settingInfo = SettingInfo(id, name, doc,
      accepts = TypeInfo.fromPatterns(overloads.map(_.accepts.toString)),
      returns = TypeInfo.fromPatterns(overloads.map(_.returns.toString)))

  def call(instance: A, request: RequestContext)(implicit ec: ExecutionContext): Future[Data] = {
    overloads.find(_.accepts.accepts(request.data.t)) match {
      case Some(handler) => handler.call(instance, request)
      case None => Future.failed(new Exception(s"no handler for type ${request.data.t}"))
    }
  }
}

/**
 * Labrad handler for methods that return synchronously.
 */
case class SyncHandler[A, B](
  params: Seq[(String, Pattern, Boolean)],
  returns: Pattern,
  invoke: (A, RequestContext) => B,
  writer: ToData[B]
) extends SimpleHandler[A] {

  val accepts: Pattern = Helper.paramPattern(params.map { case (name, pat, isOptional) => pat })

  def call(instance: A, request: RequestContext)(implicit ec: ExecutionContext): Future[Data] = {
    Future {
      checkRequest(request)
      val result = invoke(instance, request)
      writer(result)
    }
  }
}

/**
 * Labrad handler for methods that return a future.
 */
case class AsyncHandler[A, B](
  params: Seq[(String, Pattern, Boolean)],
  returns: Pattern,
  invoke: (A, RequestContext) => Future[B],
  writer: ToData[B]
) extends SimpleHandler[A] {

  val accepts: Pattern = Helper.paramPattern(params.map { case (name, pat, isOptional) => pat })

  def call(instance: A, request: RequestContext)(implicit ec: ExecutionContext): Future[Data] = {
    Future {
      checkRequest(request)
    }.flatMap { _ =>
      invoke(instance, request)
    }.map {
      writer.apply
    }
  }
}

object Helper {
  /** Combine patterns for function parameters into one pattern for the entire function. */
  def paramPattern(patterns: Seq[Pattern]): Pattern = {
    patterns match {
      case Seq() => Pattern("_")
      case Seq(pat) => pat
      case patterns => PCluster(patterns: _*)
    }
  }
}

object Macros {

  def makeHandler[T]: Seq[Handler2[T]] = macro Impl.makeHandler[T]

  // support code for the runtime implementation of labrad settings

  def extractArg[A](
    request: RequestContext,
    i: Int,
    name: String,
    reader: Getter[A],
    default: Option[() => A],
    firstParamTupleType: Option[String]
  ): A = {
    val params = request.data
    val paramOpt = if (params.isNone) {
      if (i == 0) {
        // If there is a default value, use it. Otherwise, just pass labrad NONE data and let the
        // reader try to handle it.
        if (default.isDefined) None else Some(Data.NONE)
      } else {
        None
      }
    } else if (params.isCluster) {
      // Check if the entire cluster should just go to the first type
      if (firstParamTupleType.map(t => Pattern(t).accepts(params.t)).getOrElse(false)) {
        if (i == 0) {
          Some(params)
        } else {
          None
        }
      } else {
        if (params.clusterSize > i) Some(params(i)) else None
      }
    } else {
      if (i == 0) Some(params) else sys.error(s"expected params cluster but got ${params.t}")
    }
    paramOpt match {
      case Some(a) =>
        reader.get(a)

      case None =>
        val f = default.getOrElse(sys.error(s"no default value for parameter $i"))
        f()
    }
  }

  val unitWriter = new ToData[Unit] {
    def pat = TNone
    def apply(b: DataBuilder, value: Unit): Unit = b.none()
  }

  /**
   * Class containing macro implementations. This is implemented as a
   * 'macro bundle' (http://docs.scala-lang.org/overviews/macros/bundles.html),
   * a class that takes the macro context as a parameter. This allows all
   * methods in the class to share the context, and use path-dependent types
   * from that context.
   */
  class Impl(val c: Context) {
    import c.universe._

    def makeHandler[T: c.WeakTypeTag]: c.Expr[Seq[Handler2[T]]] = {
      val t = weakTypeOf[T]
      val allMethods = t.members.toSeq.collect { case m: MethodSymbol => m }
      val overloads = allMethods.groupBy(_.name.decodedName.toString).toSeq

      // build handler for each set of overloaded methods
      val infosAndHandlers = for {
        (methodName, methods) <- overloads
        annots = methods.flatMap(settingAnnotation)
        if annots.length > 0
      } yield {
        try {
          require(annots.length == 1, s"Multiple overloads of '$methodName' have @Setting annotation")
          val (id, name, rawDoc) = annots(0)
          val doc = rawDoc.stripMargin

          val methodsWithDefaults = for (m <- methods) yield {
            val prefix = m.name.decodedName.toString + "$default$"
            val defaults = allMethods.filter(_.name.decodedName.toString.startsWith(prefix))
            (m, defaults)
          }
          val handler = handlerForMethods[T](id, name, doc, methodsWithDefaults)

          (id, name) -> handler
        } catch {
          case e: Exception =>
            throw new Exception(s"error making handler for method $methodName", e)
        }
      }
      val (infos, handlers) = infosAndHandlers.unzip

      // check that setting ids and names are unique
      for ((id, settings) <- infos.groupBy(_._1)) {
        require(settings.length == 1, s"Multiple settings with id $id in type $t")
      }
      for ((name, settings) <- infos.groupBy(_._2)) {
        require(settings.length == 1, s"Multiple settings with name '$name' in type $t")
      }

      c.Expr[Seq[Handler2[T]]](q"Seq(...$handlers)")
    }

    // extract information from an optional @Setting annotation on a symbol
    private def settingAnnotation(s: Symbol): Option[(Long, String, String)] = {
      for {
        a <- s.annotations.find(_.tree.tpe =:= typeOf[Setting])
        id <- a.param("id").map { case Constant(id: Long) => id }
        name <- a.param("name").map { case Constant(name: String) => name }
        doc <- a.param("doc").map { case Constant(doc: String) => doc }
      } yield (id, name, doc)
    }

    implicit class RichAnnotation(a: c.universe.Annotation) {
      // extract a single parameter by name
      def param(name: String): Option[Constant] = {
        a.tree.children.tail.collectFirst {
          case AssignOrNamedArg(Ident(TermName(`name`)), Literal(c)) => c
        }
      }
    }


    /** Get metadata and a handler function for a sequence of overridden methods */
    def handlerForMethods[T: c.WeakTypeTag](
      id: Long,
      name: String,
      doc: String,
      methods: Seq[(MethodSymbol, Seq[MethodSymbol])]
    ): c.Expr[Handler2[T]] = {
      require(methods.size > 0, "must specify at least one method")

      val handlers = methods.map { case (m, defaults) => methodHandler(id, name, doc, m, defaults) }

      // create a binder that binds all overloads, then tries each one in sequence
      val overloadedHandler = q"""new $overloadedHandlerT(
        id = $id,
        name = $name,
        doc = $doc,
        overloads = $seq(..${handlers.map(_.tree)})
      )"""

      c.Expr[Handler2[T]](overloadedHandler)
    }

    /** Get metadata and a handler function for a labrad method */
    /*
    def makeHandler[T: c.WeakTypeTag](
      m: MethodSymbol,
      defaultMethods: Seq[MethodSymbol]
    ): (Pattern, Pattern, c.Expr[SimpleHandler[T]]) = {
      val (types, returnType) = m.typeSignature match {
        case PolyType(args, ret) => (args.map(_.asType), ret)
        case MethodType(args, ret) => (args.map(_.asTerm), ret)
        case NullaryMethodType(ret) => (Nil, ret)
      }

      // check if the first argument is a RequestContext
      val includeRequestContext = types.nonEmpty && types(0).typeSignature <:< typeOf[RequestContext]
      val unpackTypes = if (includeRequestContext) types.tail else types
      val firstUnpackParam = if (includeRequestContext) 2 else 1

      // a sequence of optional defaults for every unpackable parameter of the method,
      // skipping the first parameter if it is the raw RequestContext
      val defaults = for (i <- firstUnpackParam to types.length) yield
        defaultMethods.find(_.name.decodedName.toString == m.name.decodedName.toString + "$default$" + i)

      // get patterns and unpackers for all method parameters
      val (patterns, unpackers) = unpackTypes.map(t => inferParamType(t.typeSignature, t.annotations)).unzip

      // modify argument patterns to allow none if the argument has a default
      val defaultPatterns = for ((p, defaultOpt) <- patterns zip defaults) yield
        if (defaultOpt.isDefined) Pattern.reduce(p, TNone) else p

      // Create patterns for combined arguments, including with trailing optional
      // args dropped. Each pattern is combined in a tuple with the number of
      // function arguments included in the pattern and the number of function
      // arguments omitted from the pattern.
      val acceptPats = patterns match {
        case Seq() => Seq((TNone, 0, 0))
        case ps    =>
          // trailing parameters are omittable if they have a default or take Option
          val omittable = (defaults zip unpackTypes).reverse.takeWhile {
            case (defaultOpt, t) =>
              defaultOpt.isDefined || isOptionType(t.typeSignature)
          }.length

          // create additional patterns with trailing optional arguments omitted
          val patterns = for (omitted <- omittable to 0 by -1) yield {
            val pattern = ps.dropRight(omitted) match {
              case Seq()  => TNone
              case Seq(p) => p
              case ps     => PCluster(ps: _*)
            }
            (pattern, ps.length - omitted, omitted)
          }

          // ensure that patterns with omitted arguments are unambiguous
          // ambiguity can occur if the first argument accepts a cluster
          // that matches another pattern with multiple arguments
          for {
            (p1, 1, _) <- patterns
            (pn, n, _) <- patterns
            if n > 1
            p <- pn.expand
          } require(!p1.accepts(p),
                s"ambiguous patterns: $p accepted with either 1 or $n arguments")

          patterns
      }
      val acceptPat = Pattern.reduce(acceptPats.map(_._1): _*)

      // get return pattern and function to repack the return value into data
      val (returnPat, repack) = inferReturnType(returnType, m.annotations)

      val binder = (self: Any) => {
        // bind default methods to this instance
        val defaultFuncs = defaults.map { _.map(method => invoke(self, method)) }

        // modify unpackers to call default methods if they receive empty data
        val defaultUnpackers = for ((unpack, defaultOpt) <- unpackers zip defaultFuncs) yield {
          defaultOpt match {
            case Some(default) => (d: Data) => if (d.isNone) default(Nil) else unpack(d)
            case None => unpack
          }
        }

        // create a function to unpack incoming data to the appropriate number of parameters,
        // and optionally include the RequestContext as a first argument
        val unpack = (defaultUnpackers, includeRequestContext) match {
          case (Seq(), true)   => (r: RequestContext) => Seq(r)
          case (Seq(), false)  => (r: RequestContext) => Seq()
          case (Seq(f), true)  => (r: RequestContext) => Seq(r, f(r.data))
          case (Seq(f), false) => (r: RequestContext) => Seq(f(r.data))
          case (fs, rc)        => (r: RequestContext) => {
            // extract data into args, adding empty for omitted args based on matching pattern
            val xs = acceptPats.find {
              case (p, _, _) => p.accepts(r.data.t)
            }.map {
              case (p, 0, k) => Seq.fill(k)(Data.NONE)
              case (p, 1, k) => Seq(r.data) ++ Seq.fill(k)(Data.NONE)
              case (p, n, k) => r.data.clusterIterator.toSeq ++ Seq.fill(k)(Data.NONE)
            }.getOrElse {
              sys.error(s"data of type ${r.data.t} not accepted")
            }
            val unpacked = (fs zip xs).map { case (f, x) => f(x) }
            if (rc) {
              r +: unpacked
            } else {
              unpacked
            }
          }
        }
        guard(acceptPat, unpack andThen invoke(self, m) andThen repack)
      }

      (acceptPat, returnPat, binder)
    }
    */

    /** infer the pattern and create an unpacker for a method parameter */
    def inferParamPattern(pos: Position, tpe: Type, annots: Seq[Annotation]): Pattern = {
      val annotatedPatternOpt = for {
        a <- annots.find(_.tree.tpe =:= typeOf[org.labrad.annotations.Accept])
        t <- a.param("value").map { case Constant(t: String) => t }
      } yield Pattern(t)
      val typePattern = patternFor(tpe, annotatedPatternOpt)

      // if we have an annotated type, make sure that it is more specific that the
      // type inferred from the scala type signature
      val pattern = annotatedPatternOpt match {
        case Some(annotatedPattern) =>
          if (!Pattern.accepts(typePattern, annotatedPattern)) {
            c.abort(pos, s"inferred type $typePattern is not compatible with annotated type $annotatedPattern")
          }
          annotatedPattern

        case None =>
          typePattern
      }

      pattern
    }

    /** infer the pattern and create a repacker for a method return type */
    def inferReturnPattern(pos: Position, tpe: Type, annots: Seq[Annotation]): Pattern = {
      val annotatedPatternOpt = for {
        a <- annots.find(_.tree.tpe =:= typeOf[org.labrad.annotations.Return])
        t <- a.param("value").map { case Constant(t: String) => t }
      } yield Pattern(t)
      val typePattern = patternFor(tpe, annotatedPatternOpt)

      // if we have an annotated type, make sure that it is more specific that the
      // type inferred from the scala type signature
      val pattern = annotatedPatternOpt match {
        case Some(annotatedPattern) =>
          if (!Pattern.accepts(typePattern, annotatedPattern)) {
            c.abort(pos, s"inferred type $typePattern is not compatible with annotated type $annotatedPattern")
          }
          annotatedPattern

        case None =>
          typePattern
      }

      pattern
    }

    /**
     * Determine whether or not the given type is an Option type.
     */
    def isOptionType(tpe: Type): Boolean = {
      tpe match {
        case TypeRef(_, Sym("Option"), Seq(t)) => true
        case _ => false
      }
    }

    /**
     * Create a handler to dispatch rpc requests to a specific method
     */
    private def methodHandler[T: c.WeakTypeTag](
      id: Long,
      name: String,
      doc: String,
      m: MethodSymbol,
      defaultMethods: Seq[MethodSymbol]
    ): c.Expr[SimpleHandler[T]] = {
      val instanceT = weakTypeOf[T]
      val methodName = symbolName(m)

      val (paramTypes, returnType) = paramAndReturnTypes(m)
      val paramNames = paramTypes.map(symbolName)

      // first param tuple types
      val firstParamTupleType: Option[String] = None

      // Create code to extract each parameter of the required type for the
      // underlying method. Note that the trees created here refer to a term
      // "params", which is defined in the invoke function below.
      val args = paramTypes.zipWithIndex.map { case (t, i) =>
        val pattern = inferParamPattern(t.pos, t.typeSignature.dealias, t.annotations)
        val reader = inferReader(t.pos, t.typeSignature.dealias)
        val defaultName = methodName + "$default$" + (i+1)
        val default = defaultMethods.find(m => symbolName(m) == defaultName) match {
          case Some(m) =>
            // Default methods are defined without parens,
            // so we can invoke them just by selecting.
            //val invokeDefault = Select(inst, TermName(defaultName))
            q"_root_.scala.Some(() => instance.${TermName(defaultName)})"

          case None => q"_root_.scala.None"
        }

        q"$extractArg(instance, params, $i, ${symbolName(t)}, $reader, $default, $firstParamTupleType)"
      }

      val invoke = q"(instance: $instanceT, params: $requestT) => instance.${TermName(methodName)}(..$args)"

      // get return value writer, and determine whether the method is async
      val resultPattern = inferReturnPattern(m.pos, returnType.dealias, m.annotations) // TODO: allow annotations directly on the return type
      val (resultWriter, async): (Tree, Boolean) = returnType.dealias match {
        case t if t <:< c.typeOf[Future[Unit]] =>
          (unitWriter, true)

        case t if t <:< c.typeOf[Future[Any]] =>
          val resultTpe = typeParams(lub(t :: c.typeOf[Future[Nothing]] :: Nil))(0)
          (inferWriter(m.pos, resultTpe), true)

        case t if t <:< c.typeOf[Unit] => (unitWriter, false)
        case t => (inferWriter(m.pos, t), false)
      }

      val handler = if (async) {
        q"""new $asyncMethodHandlerT($id, $name, $doc, $seq(..$paramNames), $invoke, $resultWriter)"""
      } else {
        q"""new $syncMethodHandlerT($id, $name, $doc, $seq(..$paramNames), $invoke, $resultWriter)"""
      }
      c.Expr[SimpleHandler[T]](handler)
    }

    /**
     * Check that a method to be used with a NOTIFY route has return type Unit
     */
    private def checkUnitReturn(m: MethodSymbol): Unit = {
      val (_, returnType) = paramAndReturnTypes(m)
      returnType.dealias match {
        case t if t <:< c.typeOf[Unit] =>
        case t =>
          // TODO: use position of route instead of method symbol
          c.abort(m.pos, s"NOTIFY method ${symbolName(m)} must return Unit")
      }
    }

    /**
     * Create a reader for the return type of a method.
     *
     * Since this is used for proxying calls to remote endpoints, we require
     * that the return type is a Future so that the method call will be
     * explicitly asynchronous.
     */
    private def resultReader(m: MethodSymbol): c.Tree = {
      val (_, returnType) = paramAndReturnTypes(m)

      returnType.dealias match {
        case t if t <:< c.typeOf[Future[Any]] =>
          val resultTpe = typeParams(lub(t :: c.typeOf[Future[Nothing]] :: Nil))(0)
          inferReader(m.pos, resultTpe)

        case t =>
          sys.error(s"CALL method ${symbolName(m)} must return a Future")
      }
    }

    /**
     * Get the parameter types and return type of the given method.
     * The parameter types are returned as symbols, which contain both
     * the parameter name and its declared type.
     */
    private def paramAndReturnTypes(m: MethodSymbol): (List[Symbol], Type) = {
      m.typeSignature match {
        case PolyType(args, ret) => (args.map(_.asType), ret)
        case MethodType(args, ret) => (args.map(_.asTerm), ret)
        case NullaryMethodType(ret) => (Nil, ret)
      }
    }

    /**
     * Get the name of a symbol as a plain old String.
     */
    private def symbolName(s: Symbol): String = s.name.decodedName.toString

    /**
     * Return a list of type parameters in the given type.
     * Example: List[(String, Int)] => Seq(Tuple2, String, Int)
     */
    private def typeParams(tpe: Type): Seq[Type] = {
      val b = Iterable.newBuilder[Type]
      tpe.foreach(b += _)
      b.result.drop(2).grouped(2).map(_.head).toIndexedSeq
    }

    /**
     * Locate an implicit Reads[T] for the given type T.
     */
    private def inferReader(pos: Position, t: Type): Tree = {
      val readerTpe = appliedType(c.typeOf[Getter[_]], List(t))
      c.inferImplicitValue(readerTpe) match {
        case EmptyTree => c.abort(pos, s"could not find implicit value of type Reads[$t]")
        case tree => tree
      }
    }

    /**
     * Locate an implicit Writes[T] for the given type T.
     */
    def inferWriter(pos: Position, t: Type): Tree = {
      val writerTpe = appliedType(c.typeOf[ToData[_]], List(t))
      c.inferImplicitValue(writerTpe) match {
        case EmptyTree => c.abort(pos, s"could not find implicit value of type Writes[$t]")
        case tree => tree
      }
    }

    // Fully-qualified symbols and types (for hygiene). Hygiene means that our
    // macro should work regardless of the environment in which it is invoked,
    // in particular the user should not have to import extra names to use it.
    // In a hygienic macro system, names we refer to here in the generated code
    // would be fully-qualified, so they would work for the person invoking the
    // macro without their needing to import those names. However, scala's
    // quasiquotes are not hygienic, so we need to make sure to use fully-
    // qualified names ourselves.

    val seq = q"_root_.scala.Seq"
    val map = q"_root_.scala.collection.immutable.Map"
    val classTag = q"_root_.scala.reflect.classTag"

    val extractArg = q"_root_.org.labrad.macros.Macros.extractArg"
    val unitWriter = q"_root_.org.labrad.macros.Macros.unitWriter"

    val requestT = tq"_root_.org.labrad.data.Data"  // TODO: need more request data, in particular enough to make a RequestContext
    val asyncMethodHandlerT = tq"_root_.org.labrad.macros.AsyncMethodHandler"
    val syncMethodHandlerT = tq"_root_.org.labrad.macros.SyncMethodHandler"
    val overloadedHandlerT = tq"_root_.org.labrad.macros.OverloadedHandler"
    val endpointInvocationHandlerT = tq"_root_.org.labrad.macros.EndpointInvocationHandler"

    def patternFor[T: TypeTag]: Pattern = patternFor(typeOf[T])

    def patternFor(tpe: Type, pat: Option[Pattern] = None): Pattern = tpe.dealias match {
      case tpe if tpe =:= typeOf[Unit]    => Pattern("_")
      case tpe if tpe =:= typeOf[Boolean] => Pattern("b")
      case tpe if tpe =:= typeOf[Int]     => Pattern("i")
      case tpe if tpe =:= typeOf[Long]    => Pattern("w")
      case tpe if tpe =:= typeOf[Double]  =>
        pat match {
          case None => Pattern("v")
          case Some(p: PValue) => p
          case Some(p) => sys.error(s"cannot infer type for $tpe with pattern $p")
        }

      case tpe if tpe =:= typeOf[Date]    => Pattern("t")
      case tpe if tpe =:= typeOf[String]  => Pattern("s")
      case tpe if tpe =:= typeOf[Data]    => Pattern("?")

      case TypeRef(_, Sym("Array") | Sym("Seq"), Seq(t)) => if (t =:= typeOf[Byte]) Pattern("y") else PArr(patternFor(t))

      case TypeRef(_, Sym("Option"), Seq(t)) => patternFor(t)

      case TypeRef(_, Sym("Either"), Seq(aL, aR)) => PChoice(patternFor(aL), patternFor(aR))

      case TypeRef(_, Sym("Tuple1"), Seq(a1)) => PCluster(patternFor(a1))
      case TypeRef(_, Sym("Tuple2"), Seq(a1, a2)) => PCluster(patternFor(a1), patternFor(a2))
      case TypeRef(_, Sym("Tuple3"), Seq(a1, a2, a3)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3))
      case TypeRef(_, Sym("Tuple4"), Seq(a1, a2, a3, a4)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4))
      case TypeRef(_, Sym("Tuple5"), Seq(a1, a2, a3, a4, a5)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5))
      case TypeRef(_, Sym("Tuple6"), Seq(a1, a2, a3, a4, a5, a6)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6))
      case TypeRef(_, Sym("Tuple7"), Seq(a1, a2, a3, a4, a5, a6, a7)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6), patternFor(a7))
      case TypeRef(_, Sym("Tuple8"), Seq(a1, a2, a3, a4, a5, a6, a7, a8)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6), patternFor(a7), patternFor(a8))
      case TypeRef(_, Sym("Tuple9"), Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6), patternFor(a7), patternFor(a8), patternFor(a9))
      case TypeRef(_, Sym("Tuple10"), Seq(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)) => PCluster(patternFor(a1), patternFor(a2), patternFor(a3), patternFor(a4), patternFor(a5), patternFor(a6), patternFor(a7), patternFor(a8), patternFor(a9), patternFor(a10))
    }

    object Sym {
      def unapply(sym: Symbol): Option[String] = Some(sym.name.decodedName.toString)
    }
  }
}
