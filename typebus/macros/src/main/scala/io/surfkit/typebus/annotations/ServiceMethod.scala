package io.surfkit.typebus.annotations

import scala.annotation.{StaticAnnotation, compileTimeOnly}
import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

@compileTimeOnly("enable macro to expand macro annotations")
class ServiceMethod extends StaticAnnotation {
  def macroTransform(annottees: Any*) = macro ServiceMethod.impl
}

object ServiceMethod {

  val serviceMethodMap = scala.collection.mutable.HashMap.empty[String, (String, String)]

  def impl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    println(s"\n\nTHE NUMBER OF ANNONS: ${annottees.size}")
    println(serviceMethodMap)

    val result =
      annottees.map(_.tree).toList match {
        case q"$mods def $methodName[..$tpes]($arg, meta: EventMeta): Future[$returnType] = { ..$body }" :: Nil =>
          //val symbol = weakTypeOf[returnType].typeSymbol
          //serviceMethodMap += methodName.fullName ->

          // https://stackoverflow.com/questions/19379436/cant-access-parents-members-while-dealing-with-macro-annotations
          val retTpe = c.typeCheck(q"(??? : $returnType)").tpe
          val argChild = arg.children.head
          val argTpe = c.typeCheck(q"(??? : $argChild)").tpe
         // val retTpe = c.typeCheck(returnType.duplicate).tpe
          println(s"retTpe:${retTpe}  ${retTpe.typeSymbol}")
          println(s"argTpe:${argTpe}  ${argTpe.typeSymbol}")

          println(s"\n\nxxx Service methode: ${methodName}[$tpes]($arg): ${returnType} ${returnType.isType}      ${arg.children.head.tpe} ${arg.children.head.symbol} ${arg.children.head.isType} ${arg.children.head.isTerm} ${arg.children}")
          q"""$mods def $methodName[..$tpes]($arg, meta: EventMeta): Future[$returnType] =  {
            val start = System.nanoTime()
            val result = {..$body}
            val end = System.nanoTime()
            println(${methodName.toString} + " elapsed time: " + (end - start) + "ns")
            result
          }
          registerStream( $methodName _ )
          """
        case _ => c.abort(c.enclosingPosition, "Annotation @ServiceMethod can be used only with methods")
      }
    c.Expr[Any](result)
  }
}