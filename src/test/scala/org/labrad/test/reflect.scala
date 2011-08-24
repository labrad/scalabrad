package org.labrad
package test

import java.io.{StringWriter, ByteArrayOutputStream, PrintStream}

import tools.scalap._
import tools.scalap.scalax.rules.scalasig._
import tools.scalap.scalax.rules.scalasig.ClassFileParser.{ConstValueIndex, Annotation}
import scala.reflect.generic.ByteCodecs
import scala.reflect.ScalaSignature


object ScalaSigReader {

  def typeRefToType(t: Type): String = t match {
    case TypeRefType(prefix, symbol, typeArgs) =>
      symbol.path + (
        if (typeArgs.isEmpty)
          ""
        else
          "[" + typeArgs.map(typeRefToType).mkString(",") + "]"
      )
    case _ =>
      ""
  }
  
  def process(clazz: Class[_]) {
    
    // iterate over child methods
    println(clazz.getName)
    val scalaSig = ScalaSigParser.parse(clazz).get
    for (sym <- scalaSig.topLevelClasses /*::: scalaSig.topLevelObjects*/) {
      println(sym.path)
      println(sym.name)
      sym.children foreach {
        case m: MethodSymbol =>
          println(m.name)
          m.infoType match {
            case NullaryMethodType(resType) => println(resType)
            case mt: MethodType =>
              mt.paramSymbols foreach {
                case ms: MethodSymbol =>
                  println("  " + ms.name + ": " + typeRefToType(ms.infoType)) // + ms.infoType)
                case _ =>
              }
              println("  => " +  typeRefToType(mt.resultType))
          }
        case _ =>
      }
    }
  }
  
  def main(args: Array[String]) = {
    process(classOf[ReflectTest])
  }
}
