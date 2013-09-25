package org.labrad.util

import scala.reflect.macros.whitebox.Context
import scala.language.experimental.macros

object Select {
  def select[A <: AnyRef](a: PartialFunction[Any, A]): PartialFunction[Any, A] = macro selectImpl[A]

  def selectImpl[A <: AnyRef](c: Context)(a: c.Expr[PartialFunction[Any, A]]) : c.Expr[PartialFunction[Any, A]] = a
}
