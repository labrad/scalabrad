package org.labrad

import org.labrad.annotations.Setting
import org.labrad.data._
import scala.collection.mutable
import scala.reflect.runtime.universe._

case class RequestContext(source: Long, context: Context, id: Long, data: Data)

object Reflect {

  def makeHandler[T: TypeTag]: (Seq[SettingInfo], T => RequestContext => Data) = {
    val t = typeOf[T]
    val allMethods = t.members.toSeq.collect { case m: MethodSymbol => m }
    val overloads = allMethods.groupBy(_.name.decodedName.toString).toSeq

    // build handler for each set of overloaded methods
    val handlers = for {
      (methodName, methods) <- overloads
      annots = methods.flatMap(settingAnnotation)
      if annots.length > 0
    } yield {
      require(annots.length == 1, s"Multiple overloads of '$methodName' have @Setting annotation")
      val (id, name, doc) = annots(0)

      val methodsWithDefaults = for (m <- methods) yield {
        val defaults = allMethods.filter(_.name.decodedName.toString.startsWith(m.name.decodedName.toString + "$default$"))
        (m, defaults)
      }
      val (accepts, returns, binder) = SettingHandler.forMethods(methodsWithDefaults)
      val acceptsInfo = TypeInfo(accepts, accepts.expand.map(_.toString))
      val returnsInfo = TypeInfo(returns, returns.expand.map(_.toString))
      val settingInfo = SettingInfo(id, name, doc.stripMargin, acceptsInfo, returnsInfo)

      (settingInfo, binder)
    }

    val infos = handlers.map { case (info, _) => info }.sortBy(_.id)
    val binders = handlers.map { case (info, binder) => info.id -> binder }.toMap

    // check that setting ids and names are unique
    for ((id, settings) <- infos.groupBy(_.id)) require(settings.length == 1, s"Multiple settings with id $id in type $t")
    for ((name, settings) <- infos.groupBy(_.name)) require(settings.length == 1, s"Multiple settings with name '$name' in type $t")

    val binder = (self: T) => {
      val handlerMap = binders.map { case (id, binder) => id -> binder(self) }
      (req: RequestContext) =>
        handlerMap.get(req.id).map(_(req)).getOrElse(Error(1, s"Setting not found: ${req.id}"))
    }
    (infos, binder)
  }


  // extract information from an optional @Setting annotation on a symbol
  private def settingAnnotation(s: Symbol): Option[(Long, String, String)] =
    for {
      a <- s.annotations.find(_.tree.tpe =:= typeOf[Setting])
      id <- a.param("id").map { case Constant(id: Long) => id }
      name <- a.param("name").map { case Constant(name: String) => name }
      doc <- a.param("doc").map { case Constant(doc: String) => doc }
    } yield (id, name, doc)

  implicit class RichAnnotation(a: Annotation) {
    // extract a single parameter by name
    def param(name: String): Option[Constant] = {
      a.tree.children.tail.collectFirst {
        case AssignOrNamedArg(Ident(TermName(`name`)), Literal(c)) => c
      }
    }
  }
}
