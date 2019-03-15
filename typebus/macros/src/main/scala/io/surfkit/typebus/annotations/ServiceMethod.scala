package io.surfkit.typebus.annotations

import java.nio.file.Files
import io.surfkit.typebus.ResourceDb
import boopickle.Default._
import scala.annotation.{StaticAnnotation, compileTimeOnly}
import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

@compileTimeOnly("enable macro to expand macro annotations")
class ServiceMethod extends StaticAnnotation {
  def macroTransform(annottees: Any*) = macro ServiceMethod.impl
}

object ServiceMethod extends ResourceDb{

  val databaseTableName = "_Service"

  sealed trait Store
  final case class ServiceMethod(in: String, out: String) extends Store
  final case class ServiceStore(methods: Set[ServiceMethod]) extends Store

  var methods = Set.empty[ServiceMethod]

  def impl(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    val result =
      annottees.map(_.tree).toList match {
        case q"$mods def $methodName[..$tpes]($arg, meta: EventMeta): Future[$returnType] = { ..$body }" :: Nil =>
        //case q"$mods def $methodName[..$tpes]($arg, meta: EventMeta): Future[$returnType]{..$body}" :: Nil =>
          // https://stackoverflow.com/questions/19379436/cant-access-parents-members-while-dealing-with-macro-annotations
          val retTpe = c.typecheck(q"(??? : $returnType)").tpe
          val argChild = arg.children.head
          val argTpe = c.typecheck(q"(??? : $argChild)").tpe
          //println(s"retTpe:${retTpe}  ${retTpe.typeSymbol.fullName}")
          //println(s"argTpe:${argTpe}  ${argTpe.typeSymbol.fullName}")

          methods += ServiceMethod(argTpe.typeSymbol.fullName, retTpe.typeSymbol.fullName)
          val servicePath = databaseTablePath(databaseTableName)
          Files.write(servicePath, serialiseServiceStore(ServiceStore(methods)))
          q"""$mods def $methodName[..$tpes]($arg, meta: EventMeta): Future[$returnType] = { ..$body }"""

        case _ => c.abort(c.enclosingPosition, s"Annotation @ServiceMethod can be used only with methods of the form (T, EventMeta) => Future[U] instead of: ${annottees.map(_.tree).toList}")
      }
    c.Expr[Any](result)
  }

  def serialiseServiceStore(value: ServiceStore): Array[Byte] =
    Pickle.intoBytes(value).array

  def deSerialiseServiceStore(bytes: Array[Byte]): ServiceStore =
    Unpickle[ServiceStore].fromBytes(java.nio.ByteBuffer.wrap(bytes))
}