package io.surfkit.typebus.macros

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.blackbox.Context

/**
  * Created by suroot on 03/01/17.
  */
class TypeBusClient(schemaFile: String) extends StaticAnnotation {

  def macroTransform(annottees: Any*) = macro QuasiquotesGenerator.generate

}


object QuasiquotesGenerator {

  def generate(c: Context)(annottees: c.Expr[Any]*) = {

    println("$$$$$$$$$$$$$$$$$$\n$$$$$$$$$$$$$$$$$$$\n$$$$$$$$$$$$$$$$$$$$$$$$$$$")

    import c.universe._

    // retrieve the schema path
    val schemaPath = c.prefix.tree match {
      case Apply(_, List(Literal(Constant(x)))) => x.toString
      case _ => c.abort(c.enclosingPosition, "schema file path is not specified")
    }

    // retrieve the annotate class name
    val (className, api) = annottees.map(_.tree) match {
      //case List(q"class $name") => name
      case List(q"object $name extends io.surfkit.typebus.client.Client[$api]($mapper)") => (name, api)
      case _ => c.abort(c.enclosingPosition, "the annotation can only be used with objects")
    }

    //println(s"api: ${api.members}")

    println(api.tpe)

    val x = q"object api extends ${api}"
    println(x)
    println(s"tpe: ${x.tpe}")

    println(showRaw(x, printTypes = true))
    println(x.children)
    //x.children.foreach(x => println(x.children) )

    //val meths = q"manifest[$api].erasure.getMethods"
    //println(meths())
  /*  val x = Class.forName(api)

    val meths = manifest[].erasure.getMethods
    println(meths take 5 mkString "\n")
*/
    // load the schema from JSON
    /*val schema = TypeSchema.fromJson(schemaPath)

    // produce the list of constructor parameters (note the "val ..." syntax)
    val params = schema.fields.map { field =>
      val fieldName = newTermName(field.name)
      val fieldType = newTypeName(field.valueType.fullName)
      q"val $fieldName: $fieldType"
    }

    val json = TypeSchema.toJson(schema)

    // rewrite the class definition
    c.Expr(
      q"""
        case class $className(..$params) {

          def schema = ${json}

        }
      """
    )*/

    val json = ""
    val params = Seq( q"val test: String" )

    c.Expr(
      q"""
        object $className {

          def schema = ${json}

        }
      """
    )
  }

}
