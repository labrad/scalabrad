/*
 * Copyright 2008 Matthew Neeley
 * 
 * This file is part of JLabrad.
 *
 * JLabrad is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 * 
 * JLabrad is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with JLabrad.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.labrad

import java.lang.reflect.{Method, GenericArrayType, ParameterizedType, Type => JType, InvocationTargetException}

import scala.collection._
import scala.tools.scalap._
import scala.tools.scalap.scalax.rules.scalasig
import scala.tools.scalap.scalax.rules.scalasig.{Symbol, MethodSymbol, MethodType, NullaryMethodType, TypeRefType}

import grizzled.slf4j.Logging

import annotations.Setting
import annotations.Matchers._
import data._
import types._


case class SettingInfo(
    id: Long,
    name: String,
    doc: String,
    accepted: Seq[TypeDescriptor],
    returned: Seq[TypeDescriptor]) {
  def accepts(typ: Type): Boolean = accepted exists { _.accepts(typ) }
  def accepts(data: Data): Boolean = accepts(data.t)
  def registrationInfo =
    Cluster(
      Word(id),
      Str(name),
      Str(doc),
      Arr(accepted map { td => Str(td.tag) }),
      Arr(returned map { td => Str(td.tag) }),
      Str(""))
}


trait SettingHandler extends Function2[Any, Data, Data] {
  val info: Setting
  val accepts: Seq[TypeDescriptor]
  val returns: Seq[TypeDescriptor]
  def isDefinedAt(data: Data): Boolean = accepts exists { _.accepts(data.t) }
  def registrationInfo =
    Cluster(
      Word(info.id),
      Str(info.name),
      Str(info.doc),
      Arr(accepts map { td => Str(td.tag) }),
      Arr(returns map { td => Str(td.tag) }),
      Str(""))
  def apply(context: Any, data: Data): Data
}

trait MethodHandler extends SettingHandler {
  val method: Method
  def input(data: Data): Seq[Any] = Seq(data)
  def output(ans: Any): Data = ans.asInstanceOf[Data]
  def apply(context: Any, data: Data): Data = {
    val args = input(data) map { _.asInstanceOf[AnyRef] }
    val ans = try {
      method.invoke(context.asInstanceOf[AnyRef], args: _*)
    } catch {
      case e: InvocationTargetException => throw e.getCause
    }
    output(ans)
  }
}

trait ZeroArg {
  this: MethodHandler =>
  val accepts = Seq(TypeDescriptor(""))
  override def apply(context: Any, data: Data): Data = {
    val ans = try {
      method.invoke(context.asInstanceOf[AnyRef])
    } catch {
      case e: InvocationTargetException => throw e.getCause
    }
    output(ans)
  }
}

trait SingleArg {
  this: MethodHandler =>
  val getter: Data => Any
  override def input(data: Data): Seq[Any] = Seq(getter(data))
}

trait MultiArg {
  this: MethodHandler =>
  val getters: Seq[Data => Any]
  override def input(data: Data): Seq[Any] =
    for (i <- 0 until getters.length) yield {
      val getter = getters(i)
      val arg = data(i)
      getter(arg)
    }
}

trait VoidOutput {
  this: MethodHandler =>
  val returns = Seq(TypeDescriptor(""))
  override def output(ans: Any): Data = Data.EMPTY
}

class MultiArgHandler(
  val method: Method,
  val info: Setting,
  val accepts: Seq[TypeDescriptor],
  val returns: Seq[TypeDescriptor],
  val getters: Seq[Data => Any])
    extends MethodHandler with MultiArg

class MultiArgVoidHandler(
  val method: Method,
  val info: Setting,
  val accepts: Seq[TypeDescriptor],
  val getters: Seq[Data => Any])
    extends MethodHandler with MultiArg with VoidOutput

class SingleArgHandler(
  val method: Method,
  val info: Setting,
  val accepts: Seq[TypeDescriptor],
  val returns: Seq[TypeDescriptor],
  val getter: Data => Any)
    extends MethodHandler with SingleArg

class SingleArgVoidHandler(
  val method: Method,
  val info: Setting,
  val accepts: Seq[TypeDescriptor],
  val getter: Data => Any)
    extends MethodHandler with SingleArg with VoidOutput

class ZeroArgHandler(
  val method: Method,
  val info: Setting,
  val returns: Seq[TypeDescriptor])
    extends MethodHandler with ZeroArg

class ZeroArgVoidHandler(
  val method: Method,
  val info: Setting)
    extends MethodHandler with ZeroArg with VoidOutput

class OverloadedSettingHandler(
  val info: Setting,
  val accepts: Seq[TypeDescriptor],
  val returns: Seq[TypeDescriptor],
  handlers: Seq[SettingHandler])
    extends SettingHandler {
  override def apply(context: Any, data: Data): Data = {
    handlers find { _.isDefinedAt(data) } match {
      case Some(handler) => handler(context, data).asInstanceOf[Data]
      case None => throw new RuntimeException("No handler found for type '" + data.tag + "'")
    }
  }
}

object SettingHandler extends Logging {
  
  /**
   * Create a SettingHandler for a method or set of overridden methods
   * @param s annotation containing 
   * @param overloads
   * @return
   */
  def forMethods(s: Setting, overloads: Seq[(Method, Option[MethodSymbol])]): SettingHandler = {
    // TODO add returned types to typed handlers since they may need to flatten objects before sending them

    debug("creating setting handler for '" + s.name + "'")
    
    // for registration with labrad, we need to keep track of the full list
    // of accepted and returned types across all overloads of this method
    val accepts = mutable.Buffer.empty[TypeDescriptor] // list of all accepted types
    val returns = mutable.Buffer.empty[TypeDescriptor] // list of all returned types
    
    val handlers = for ((m, symbol) <- overloads) yield {
      // get a handler for this overload
      val handler = getHandler(s, m, symbol)
            
      // get accepted types for this overload
      // new accepted types will be kept if there is not already a more
      // general type that accepts (is more general than) the new type
      for (t <- handler.accepts)
        if (!accepts.exists(_ accepts t )) accepts += t
      
      // get returned types for this overload
      // new return types will be kept if there is not already a more
      // general type that accepts (is more general than) the new type
      for (t <- handler.returns)
        if (!returns.exists(_ accepts t)) returns += t
      
      handler
    }
    
    if (handlers.size == 1) {
      // just use the unique handler
      // need to specify return types here
      handlers(0)

    } else {
      // make a map from all types to the appropriate handlers
      new OverloadedSettingHandler(s, accepts.toSeq, returns.toSeq, handlers)
    }
  }


  /**
   * Get a handler for a particular method, along with a list of all
   * accepted types for the method (since each parameter may accept multiple types) 
   * @param m
   * @param s
   * @return
   */
  def getHandler(s: Setting, m: Method, symbol: Option[MethodSymbol]): SettingHandler = {
    // each parameter to the method has a type that we can infer,
    // as well as optional types from the @Accepts annotation

    val jTypes = m.getGenericParameterTypes.toSeq
    val jAnnots = m.getParameterAnnotations.toSeq.map(_.toSeq)
    val sTypes = symbol match {
      case Some(symbol) =>
        symbol.infoType match {
          case NullaryMethodType(_) =>
            Seq()
          case mt: MethodType =>
            mt.paramSymbols map { case ms: MethodSymbol => Some(ms) }
        }
      case None =>
        jTypes map { _ => None }
        
    }
    assert(jTypes.size == sTypes.size, "must have same number of java and scala parameters")
    val params = jTypes zip jAnnots zip sTypes
    
    val typesAndGetters = for (((cls, annots), symbol) <- params) yield {
      // infer the type of each argument and create a getter for it
      debug("inferring type: " + cls + ", " + annots + ", " + symbol)
      val (inferredType, getter) = symbol match {
        case Some(symbol) =>
          inferType(symbol.infoType)
        case None =>
          inferType(cls)
      }
      
      // For all @Accepts annotations on this parameter,
      // check that accepted types are compatible with inferred type.
      // If there is no such annotation, use the inferred type
      var accepts = for (Accepts(tag) <- annots) yield TypeDescriptor(tag)
      if (accepts.isEmpty) accepts = Seq(TypeDescriptor(inferredType))
      
      // check that the inferred type accepts all annotatation-specified types
      for (td <- accepts)
        require(inferredType accepts td.typ,
          "Accepted type '%s' does not match inferred type '%s'".format(td.tag, inferredType))
      
      // return accepted types and getter
      (accepts, getter)
    }
    val (paramTypes, getters) = typesAndGetters.unzip
    
    // create a handler of the appropriate type for this setting
    val numIn = paramTypes.length
    val numOut = m.getReturnType match { case VoidType() => 0; case _ => 1 }    
    val accepts = getAcceptedTypeCombinations(paramTypes)
    val returns = getReturnedTypes(m)
    
    debug("method: " + m)
    debug("accepts: " + accepts.mkString(" | "))
    debug("returns: " + returns.mkString(" | "))
    
    (numIn, numOut) match {
      case (0, 0) => new ZeroArgVoidHandler(m, s)
      case (0, 1) => new ZeroArgHandler(m, s, returns)

      case (1, 0) => new SingleArgVoidHandler(m, s, accepts, getters(0))
      case (1, 1) => new SingleArgHandler(m, s, accepts, returns, getters(0))

      case (_, 0) => new MultiArgVoidHandler(m, s, accepts, getters)
      case (_, 1) => new MultiArgHandler(m, s, accepts, returns, getters)
    }
  }
  
  private object BooleanType {
    def unapply(cls: JType): Boolean =
      (cls == classOf[Boolean]) || (cls == classOf[java.lang.Boolean])
  }
  
  private object IntType {
    def unapply(cls: JType): Boolean =
      (cls == classOf[Int]) || (cls == classOf[java.lang.Integer])
  }
  
  private object LongType {
    def unapply(cls: JType): Boolean =
      (cls == classOf[Long]) || (cls == classOf[java.lang.Long])
  }
  
  private object DoubleType {
    def unapply(cls: JType): Boolean =
      (cls == classOf[Double]) || (cls == classOf[java.lang.Double])
  }
  
  private object StringType {
    def unapply(cls: JType): Boolean = cls == classOf[String]
  }
  
  private object DataType {
    def unapply(cls: JType): Boolean = cls == classOf[Data]
  }
  
  private object VoidType {
    def unapply(cls: JType): Boolean =
      (cls == classOf[Void]) || (cls.toString == "void")
  }
  
  private object BooleanArrayType {
    def unapply(cls: JType): Boolean = cls == classOf[Array[Boolean]]
  }
  
  private object ByteArrayType {
    def unapply(cls: JType): Boolean = cls == classOf[Array[Byte]]
  }
  
  private object IntArrayType {
    def unapply(cls: JType): Boolean = cls == classOf[Array[Int]]
  }
  
  private object LongArrayType {
    def unapply(cls: JType): Boolean = cls == classOf[Array[Long]]
  }
  
  private object StringArrayType {
    def unapply(cls: JType): Boolean = cls == classOf[Array[String]]
  }
  
  private object DoubleArrayType {
    def unapply(cls: JType): Boolean = cls == classOf[Array[Double]]
  }
  
  private object ArrayType {
    def unapply(cls: JType): Option[JType] =
      if (cls.isInstanceOf[GenericArrayType])
        Some(cls.asInstanceOf[GenericArrayType].getGenericComponentType)
      else
        None
  }
  
  private object ParamType {
    def unapply(cls: JType): Option[(JType, Array[JType])] = {
      if (cls.isInstanceOf[ParameterizedType]) {
        val paramCls = cls.asInstanceOf[ParameterizedType]
        Some((paramCls.getRawType, paramCls.getActualTypeArguments))
      } else {
        None
      }
    }
  }
  
  private object OptionType {
    def unapply(cls: JType): Option[JType] = cls match {
      case ParamType(cls, Array(param)) =>
        if (cls == classOf[Option[_]])
          Some(param)
        else
          None
      case _ => None
    }
    
//    def unapply(arg: scalasig.Type): Option[scalasig.Type] = arg match {
//      case TypeRefType(prefix, symbol, Seq(t)) if symbol.path == "scala.Option" =>
//        Some(t)
//      case _ =>
//        None
//    }
  }
  
  private object EitherType {
    def unapply(cls: JType): Option[(JType, JType)] = cls match {
      case ParamType(cls, Array(c1, c2)) =>
        if (cls == classOf[Either[_, _]])
          Some((c1, c2))
        else
          None
      case _ => None
    }
  }
  
  private object SeqType {
    def unapply(cls: JType): Option[JType] = cls match {
      case ParamType(cls, Array(param)) =>
        if (cls == classOf[Seq[_]])
          Some(param)
        else
          None
      case _ => None
    }
  }
  
  private object Tuple2Type {
    def unapply(cls: JType): Option[(JType, JType)] = cls match {
      case ParamType(cls, Array(c1, c2)) =>
        if (cls == classOf[Tuple2[_, _]])
          Some((c1, c2))
        else
          None
      case _ => None
    }
  }
  
  private object Tuple3Type {
    def unapply(cls: JType): Option[(JType, JType, JType)] = cls match {
      case ParamType(cls, Array(c1, c2, c3)) =>
        if (cls == classOf[Tuple3[_, _, _]])
          Some((c1, c2, c3))
        else
          None
      case _ => None
    }
  }
  
  private object Tuple4Type {
    def unapply(cls: JType): Option[(JType, JType, JType, JType)] = cls match {
      case ParamType(cls, Array(c1, c2, c3, c4)) =>
        if (cls == classOf[Tuple4[_, _, _, _]])
          Some((c1, c2, c3, c4))
        else
          None
      case _ => None
    }
  }
  
  private object Tuple5Type {
    def unapply(cls: JType): Option[(JType, JType, JType, JType, JType)] = cls match {
      case ParamType(cls, Array(c1, c2, c3, c4, c5)) =>
        if (cls == classOf[Tuple5[_, _, _, _, _]])
          Some((c1, c2, c3, c4, c5))
        else
          None
      case _ => None
    }
  }
  
  private object Tuple6Type {
    def unapply(cls: JType): Option[(JType, JType, JType, JType, JType, JType)] = cls match {
      case ParamType(cls, Array(c1, c2, c3, c4, c5, c6)) =>
        if (cls == classOf[Tuple6[_, _, _, _, _, _]])
          Some((c1, c2, c3, c4, c5, c6))
        else
          None
      case _ => None
    }
  }
  
  def inferType(cls: java.lang.reflect.Type): (Type, Data => _) = {
    cls match {
      case BooleanType() => (Type("b"), (d: Data) => d.getBool)
      case IntType() => (Type("i"), (d: Data) => d.getInt)
      case LongType() => (Type("w"), (d: Data) => d.getWord)
      case StringType() => (Type("s"), (d: Data) => d.getString)
      case DoubleType() => (Type("v"), (d: Data) => d.getValue)
      case DataType() => (Type("?"), (d: Data) => d)

      // Array
      case ByteArrayType() => (Type("s"), (d: Data) => d.getBytes)
      case BooleanArrayType() => (Type("*b"), (d: Data) => d.getBoolArray)
      case IntArrayType() => (Type("*i"), (d: Data) => d.getIntArray)
      case LongArrayType() => (Type("*w"), (d: Data) => d.getWordArray)
      case DoubleArrayType() => (Type("*v"), (d: Data) => d.getValueArray)
      case StringArrayType() => (Type("*s"), (d: Data) => d.getStringArray)
      case ArrayType(DataType()) => (Type("*?"), (d: Data) => d.getDataArray)
      case ArrayType(cls) =>
        val (typ, f) = inferType(cls)
        (TArr(typ), (data: Data) => {
          (for (i <- 0 until data.getArraySize) yield f(data(i))).toArray
        })

      // Seq
      case SeqType(BooleanType()) => (Type("*b"), (d: Data) => d.getBoolSeq)
      case SeqType(IntType()) => (Type("*i"), (d: Data) => d.getIntSeq)
      case SeqType(LongType()) => (Type("*w"), (d: Data) => d.getWordSeq)
      case SeqType(StringType()) => (Type("*s"), (d: Data) => d.getStringSeq)
      case SeqType(DataType()) => (Type("*?"), (d: Data) => d.getDataSeq)
      case SeqType(cls) =>
        val (typ, f) = inferType(cls)
        (TArr(typ), (data: Data) => {
          for (i <- 0 until data.getArraySize) yield f(data(i))
        })
      
      // optional argument
      case OptionType(cls) =>
        val (typ, f) = inferType(cls)
        val optionGetter = (data: Data) => Some(f(data))
        (typ, optionGetter)
      
      // either
      case EitherType(cL, cR) =>
        val (tL, fL) = inferType(cL)
        val (tR, fR) = inferType(cR)
        (TChoice(tL, tR), (data: Data) => {
          if (tL accepts (data.t))
            Left(fL(data))
          else
            Right(fR(data))
        })
        
      // tuples
      case Tuple2Type(c1, c2) =>
        val (t1, f1) = inferType(c1)
        val (t2, f2) = inferType(c2)
        (TCluster(t1, t2), (data: Data) => data match {
          case Cluster(d1, d2) => (f1(d1), f2(d2))
        })
      
      case Tuple3Type(c1, c2, c3) =>
        val (t1, f1) = inferType(c1)
        val (t2, f2) = inferType(c2)
        val (t3, f3) = inferType(c3)
        (TCluster(t1, t2, t3), (data: Data) => data match {
          case Cluster(d1, d2, d3) => (f1(d1), f2(d2), f3(d3))
        })
      
      case Tuple4Type(c1, c2, c3, c4) =>
        val (t1, f1) = inferType(c1)
        val (t2, f2) = inferType(c2)
        val (t3, f3) = inferType(c3)
        val (t4, f4) = inferType(c4)
        (TCluster(t1, t2, t3, t4), (data: Data) => data match {
          case Cluster(d1, d2, d3, d4) => (f1(d1), f2(d2), f3(d3), f4(d4))
        })
      
      case Tuple5Type(c1, c2, c3, c4, c5) =>
        val (t1, f1) = inferType(c1)
        val (t2, f2) = inferType(c2)
        val (t3, f3) = inferType(c3)
        val (t4, f4) = inferType(c4)
        val (t5, f5) = inferType(c5)
        (TCluster(t1, t2, t3, t4, t5), (data: Data) => data match {
          case Cluster(d1, d2, d3, d4, d5) => (f1(d1), f2(d2), f3(d3), f4(d4), f5(d5))
        })
        
      case Tuple6Type(c1, c2, c3, c4, c5, c6) =>
        val (t1, f1) = inferType(c1)
        val (t2, f2) = inferType(c2)
        val (t3, f3) = inferType(c3)
        val (t4, f4) = inferType(c4)
        val (t5, f5) = inferType(c5)
        val (t6, f6) = inferType(c6)
        (TCluster(t1, t2, t3, t4, t5, t6), (data: Data) => data match {
          case Cluster(d1, d2, d3, d4, d5, d6) => (f1(d1), f2(d2), f3(d3), f4(d4), f5(d5), f6(d6))
        })
        
      case _ => throw new Exception("no match for " + cls)
    }
  }
  
  def inferType(arg: scalasig.Type): (Type, Data => _) = {
    arg match {
      case TypeRefType(prefix, symbol, typeArgs) => symbol.path match {
        case "scala.Boolean" => (Type("b"), (d: Data) => d.getBool)
        case "scala.Int" => (Type("i"), (d: Data) => d.getInt)
        case "scala.Long" => (Type("w"), (d: Data) => d.getWord)
        case "scala.Predef.String" => (Type("s"), (d: Data) => d.getString)
        case "scala.Double" => (Type("v"), (d: Data) => d.getValue)
        case "org.labrad.data.Data" => (Type("?"), (d: Data) => d)
  
        // Array
        case "scala.Array" =>
          typeArgs.head match {
            case arg @ TypeRefType(prefix, symbol, typeArgs) => symbol.path match {
              case "scala.Byte" => (Type("s"), (d: Data) => d.getBytes)
              case "scala.Boolean" => (Type("*b"), (d: Data) => d.getBoolArray)
              case "scala.Int" => (Type("*i"), (d: Data) => d.getIntArray)
              case "scala.Long" => (Type("*w"), (d: Data) => d.getWordArray)
              case "scala.Double" => (Type("*v"), (d: Data) => d.getValueArray)
              case "scala.Predef.String" => (Type("*s"), (d: Data) => d.getStringArray)
              case "org.labrad.data.Data" => (Type("*?"), (d: Data) => d.getDataArray)
              case _ =>
                val (typ, f) = inferType(arg)
                (TArr(typ), (data: Data) => {
                  (for (i <- 0 until data.getArraySize) yield f(data(i))).toArray
                })
            }
          }
  
        // Seq
        case "scala.package.Seq" =>
          typeArgs.head match {
            case arg @ TypeRefType(prefix, symbol, typeArgs) => symbol.path match {
              case "scala.Byte" => (Type("s"), (d: Data) => d.getBytes.toSeq)
              case "scala.Boolean" => (Type("*b"), (d: Data) => d.getBoolSeq)
              case "scala.Int" => (Type("*i"), (d: Data) => d.getIntSeq)
              case "scala.Long" => (Type("*w"), (d: Data) => d.getWordSeq)
              case "scala.Predef.String" => (Type("*s"), (d: Data) => d.getStringSeq)
              case "org.labrad.data.Data" => (Type("*?"), (d: Data) => d.getDataSeq)
              case _ =>
                // TODO: support inhomogeneous datatypes as TExpando
                val (typ, f) = inferType(arg)
                (TArr(typ), (data: Data) => {
                  (for (i <- 0 until data.getArraySize) yield f(data(i)))
                })
            }
          }
        
        // optional argument
        case "scala.Option" =>
          typeArgs.head match {
            case arg @ TypeRefType(prefix, symbol, typeArgs) =>
              val (typ, f) = inferType(arg)
              val optionGetter = (data: Data) => Some(f(data))
              (typ, optionGetter)
          }
        
        // either
        case "scala.Either" =>
          typeArgs match {
            case Seq(aL, aR) =>
              val (tL, fL) = inferType(aL)
              val (tR, fR) = inferType(aR)
              (TChoice(tL, tR), (data: Data) => {
                if (tL accepts (data.t))
                  Left(fL(data))
                else
                  Right(fR(data))
              })
          }
        
        // tuples
        case "scala.Tuple1" =>
          typeArgs match {
            case Seq(a1) =>
              val (t1, f1) = inferType(a1)
              (TCluster(t1), (data: Data) => data match {
                case Cluster(d1) => Tuple1(f1(d1))
              })
          }
          
        case "scala.Tuple2" =>
          typeArgs match {
            case Seq(a1, a2) =>
              val (t1, f1) = inferType(a1)
              val (t2, f2) = inferType(a2)
              (TCluster(t1, t2), (data: Data) => data match {
                case Cluster(d1, d2) => (f1(d1), f2(d2))
              })
          }
          
        case "scala.Tuple3" =>
          typeArgs match {
            case Seq(a1, a2, a3) =>
              val (t1, f1) = inferType(a1)
              val (t2, f2) = inferType(a2)
              val (t3, f3) = inferType(a3)
              (TCluster(t1, t2, t3), (data: Data) => data match {
                case Cluster(d1, d2, d3) => (f1(d1), f2(d2), f3(d3))
              })
          }
        
        case "scala.Tuple4" =>
          typeArgs match {
            case Seq(a1, a2, a3, a4) =>
              val (t1, f1) = inferType(a1)
              val (t2, f2) = inferType(a2)
              val (t3, f3) = inferType(a3)
              val (t4, f4) = inferType(a4)
              (TCluster(t1, t2, t3, t4), (data: Data) => data match {
                case Cluster(d1, d2, d3, d4) => (f1(d1), f2(d2), f3(d3), f4(d4))
              })
          }
          
        case "scala.Tuple5" =>
          typeArgs match {
            case Seq(a1, a2, a3, a4, a5) =>
              val (t1, f1) = inferType(a1)
              val (t2, f2) = inferType(a2)
              val (t3, f3) = inferType(a3)
              val (t4, f4) = inferType(a4)
              val (t5, f5) = inferType(a5)
              (TCluster(t1, t2, t3, t4, t5), (data: Data) => data match {
                case Cluster(d1, d2, d3, d4, d5) => (f1(d1), f2(d2), f3(d3), f4(d4), f5(d5))
              })
          }
          
        case "scala.Tuple6" =>
          typeArgs match {
            case Seq(a1, a2, a3, a4, a5, a6) =>
              val (t1, f1) = inferType(a1)
              val (t2, f2) = inferType(a2)
              val (t3, f3) = inferType(a3)
              val (t4, f4) = inferType(a4)
              val (t5, f5) = inferType(a5)
              val (t6, f6) = inferType(a6)
              (TCluster(t1, t2, t3, t4, t5, t6), (data: Data) => data match {
                case Cluster(d1, d2, d3, d4, d5, d6) => (f1(d1), f2(d2), f3(d3), f4(d4), f5(d5), f6(d6))
              })
          }
          
        case _ => throw new Exception("no match for " + arg)
      }
    }
  }
  
//  def inferType(cls: scala.tools.scalap.scalax.rules.scalasig.Type): (Type, Data => _) = {
//    cls match {
//      case BooleanType() => (Type("b"), (d: Data) => d.getBool)
//      case IntType() => (Type("i"), (d: Data) => d.getInt)
//      case LongType() => (Type("w"), (d: Data) => d.getWord)
//      case StringType() => (Type("s"), (d: Data) => d.getString)
//      case DoubleType() => (Type("v"), (d: Data) => d.getValue)
//      case DataType() => (Type("?"), (d: Data) => d)
//
//      // Array
//      case ByteArrayType() => (Type("s"), (d: Data) => d.getBytes)
//      case BooleanArrayType() => (Type("*b"), (d: Data) => d.getBoolArray)
//      case IntArrayType() => (Type("*i"), (d: Data) => d.getIntArray)
//      case LongArrayType() => (Type("*w"), (d: Data) => d.getWordArray)
//      case DoubleArrayType() => (Type("*v"), (d: Data) => d.getValueArray)
//      case StringArrayType() => (Type("*s"), (d: Data) => d.getStringArray)
//      case ArrayType(DataType()) => (Type("*?"), (d: Data) => d.getDataArray)
//      case ArrayType(cls) =>
//        val (typ, f) = inferType(cls)
//        (TArr(typ), (data: Data) => {
//          (for (i <- 0 until data.getArraySize) yield f(data(i))).toArray
//        })
//
//      // Seq
//      case SeqType(BooleanType()) => (Type("*b"), (d: Data) => d.getBoolSeq)
//      case SeqType(IntType()) => (Type("*i"), (d: Data) => d.getIntSeq)
//      case SeqType(LongType()) => (Type("*w"), (d: Data) => d.getWordSeq)
//      case SeqType(StringType()) => (Type("*s"), (d: Data) => d.getStringSeq)
//      case SeqType(DataType()) => (Type("*?"), (d: Data) => d.getDataSeq)
//      case SeqType(cls) =>
//        val (typ, f) = inferType(cls)
//        (TArr(typ), (data: Data) => {
//          for (i <- 0 until data.getArraySize) yield f(data(i))
//        })
//      
//      // optional argument
//      case OptionType(cls) =>
//        val (typ, f) = inferType(cls)
//        val optionGetter = (data: Data) => Some(f(data))
//        (typ, optionGetter)
//      
//      // either
//      case EitherType(cL, cR) =>
//        val (tL, fL) = inferType(cL)
//        val (tR, fR) = inferType(cR)
//        (TChoice(tL, tR), (data: Data) => {
//          if (tL accepts (data.t))
//            Left(fL(data))
//          else
//            Right(fR(data))
//        })
//        
//      // tuples
//      case Tuple2Type(c1, c2) =>
//        val (t1, f1) = inferType(c1)
//        val (t2, f2) = inferType(c2)
//        (TCluster(t1, t2), (data: Data) => data match {
//          case Cluster(d1, d2) => (f1(d1), f2(d2))
//        })
//      
//      case Tuple3Type(c1, c2, c3) =>
//        val (t1, f1) = inferType(c1)
//        val (t2, f2) = inferType(c2)
//        val (t3, f3) = inferType(c3)
//        (TCluster(t1, t2, t3), (data: Data) => data match {
//          case Cluster(d1, d2, d3) => (f1(d1), f2(d2), f3(d3))
//        })
//      
//      case Tuple4Type(c1, c2, c3, c4) =>
//        val (t1, f1) = inferType(c1)
//        val (t2, f2) = inferType(c2)
//        val (t3, f3) = inferType(c3)
//        val (t4, f4) = inferType(c4)
//        (TCluster(t1, t2, t3, t4), (data: Data) => data match {
//          case Cluster(d1, d2, d3, d4) => (f1(d1), f2(d2), f3(d3), f4(d4))
//        })
//      
//      case Tuple5Type(c1, c2, c3, c4, c5) =>
//        val (t1, f1) = inferType(c1)
//        val (t2, f2) = inferType(c2)
//        val (t3, f3) = inferType(c3)
//        val (t4, f4) = inferType(c4)
//        val (t5, f5) = inferType(c5)
//        (TCluster(t1, t2, t3, t4, t5), (data: Data) => data match {
//          case Cluster(d1, d2, d3, d4, d5) => (f1(d1), f2(d2), f3(d3), f4(d4), f5(d5))
//        })
//        
//      case Tuple6Type(c1, c2, c3, c4, c5, c6) =>
//        val (t1, f1) = inferType(c1)
//        val (t2, f2) = inferType(c2)
//        val (t3, f3) = inferType(c3)
//        val (t4, f4) = inferType(c4)
//        val (t5, f5) = inferType(c5)
//        val (t6, f6) = inferType(c6)
//        (TCluster(t1, t2, t3, t4, t5, t6), (data: Data) => data match {
//          case Cluster(d1, d2, d3, d4, d5, d6) => (f1(d1), f2(d2), f3(d3), f4(d4), f5(d5), f6(d6))
//        })
//        
//      case _ => throw new Exception("no match for " + cls)
//    }
//  }
  
  /**
   * Get overall accepted types given types accepted for each parameter.
   * @param paramAccepts
   * @return
   */
  def getAcceptedTypeCombinations(paramAccepts: Seq[Seq[TypeDescriptor]]) = {
    /*
    def choices(tds: Array[TypeDescriptor]) = tds match {
      case Array(td) => td.typ
      case tds => TChoice(tds.map(_.typ): _*)
    }
    
    val typ = paramAccepts.map(choices) match {
      case Array(typ) => typ
      case types => TCluster(types: _*)
    }
    
    TypeDescriptor(typ)
    */
    
    def combinations[T](lists: List[List[T]]): List[List[T]] = lists match {
      case Nil => List(Nil)
      case heads :: rest =>
        val tails = combinations(rest)
        for (head <- heads; tail <- tails) yield head :: tail
    }
    
    
    
    val combs = for (types <- combinations(paramAccepts.map(arr => arr.flatMap(_.typ.expanded).toList).toList)) yield {
      // XXX need an intelligent way to combine tags here
      //val tag = types.map(_.typ.toString).mkString(", ")
      //val description = types.map(_.tag).mkString(", ")
      val tag = types.map(_.toString).mkString(", ")
      TypeDescriptor(tag) // + ": " + description)
    }
    combs.toSeq
  }
  

  /**
   * Get a list of all returned LabRAD types for a method
   * @param m
   * @return
   */
  private def getReturnedTypes(m: Method): Seq[TypeDescriptor] = {
    // TODO check Java return types and provide setters for them if possible
    // TODO if there is an annotation and inferred types, make sure they are compatible
    
    val types = for (Returns(tag) <- m.getAnnotations) yield TypeDescriptor(tag)
    
    if (types.isEmpty)
      Seq(TypeDescriptor(m.getReturnType match {
        case DataType() => "?"
        case VoidType() => ""
      }))
    else
      types.toSeq.flatMap(_.typ.expanded).map(TypeDescriptor(_))
  }

  // TODO new "unpack" annotation for unpacking arbitrary types
  // TODO allow Option types to be omitted
  // TODO handle arguments that have default values
}
