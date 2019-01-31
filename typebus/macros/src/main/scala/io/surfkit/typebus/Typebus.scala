package io.surfkit.typebus

import java.nio.file.{Files, Paths}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import boopickle.Default._

/***
  * Schemacha - cheeky name for Schema wrapper
  */
trait Schemacha{
  def schema: String
}

/***
  * ByteStreamWriter - define a class that knows how to serialize a type A
  * @tparam A - the type to be serialized
  */
trait ByteStreamWriter[A] extends Schemacha{
  /***
    * write - take a type A to Array[Byte]
    * @param value of type A
    * @return Array[Byte]
    */
  def write(value: A): Array[Byte]
}

/***
  * ByteStreamReader - define a class that knows how to read Array[Byte] and make a type A
  * @tparam A - the type A
  */
trait ByteStreamReader[A] extends Schemacha{
  /***
    * read - take an Array[Byte] and make a type A
    * @param bytes - Array[Byte]
    * @return - a type A
    */
  def read(bytes: Array[Byte]): A
}


/***
  * Returned type from a successful macro expantion.  This is what should be an implicit in client code that
  * will provide serializations.
  * @param reader - the client ByteStreamReader interface
  * @param writer - the client ByteStreamWriter interface
  * @tparam A - the type A that we want to read and write
  */
class ByteStreamReaderWriter[A](reader: ByteStreamReader[A], writer: ByteStreamWriter[A] )
  extends ByteStreamReader[A] with ByteStreamWriter[A]{

  def write(value: A): Array[Byte] = writer.write(value)
  def read(bytes: Array[Byte]): A = reader.read(bytes)
  def schema: String = reader.schema
}


object Typebus{
  import scala.language.experimental.macros
  import scala.reflect.macros.blackbox

  /***
    * Helper types to create a AST of only the parts we care about
    */
  sealed trait Term{
    def `type`: String
  }

  /***
    * This type represents the AST type for a case class property
    * @param term - The AST type for the propery
    * @param hasDefault - if there as a default value provided or not.
    */
  case class Property(term: Term, hasDefault: Boolean)

  /***
    * Terminator Node that contains a Type
    * @param `type` - the type for this terminator
    */
  case class Leaf(`type`: String) extends Term

  /***
    * This is a Complext type (class, case class)
    * @param `type` - the type of the case class
    * @param members - all the members declared in the case class (mapping of "name" -> Property)
    */
  case class Node(`type`: String, members: Map[String, Property]) extends Term

  /***
    * Type containers with a single Type parameter, eg: Option, Seq, ...
    * @param `type` - The type of container
    * @param contains - The type "inside" the container.
    */
  case class MonoContainer(`type`: String, contains: Term) extends Term

  /***
    * Type container with 2 Type parameters, eg: Either, Map
    * @param `type` - the type of the container
    * @param left - left "contained" type
    * @param right - right "contained" type
    */
  case class BiContainer(`type`: String, left: Term, right: Term) extends Term

  /***
    * Helper class to "flatten" and object into scoped paths
    * @param path - fully qualified name path
    * @param hasDefault - if this contains a default value
    */
  case class PropScope(path: String, hasDefault: Boolean)

  val supportedBaseTypes = Set(
    "scala.String",
    "java.lang.String",
    "scala.Int",
    "java.lang.Int",
    "scala.Double",
    "java.lang.Double",
    "scala.Float",
    "java.lang.Float",
    "scala.Long",
    "java.lang.Long",
    "scala.Boolean",
    "java.lang.Boolean",
    "scala.Byte",
    "java.lang.Byte",
    "scala.Char",
    "java.lang.Char",
    "BigDecimal",
    "java.math.BigDecimal",
    "org.joda.time.DateTime",
    "org.joda.time.LocalDate",
    "java.util.UUID"
  )

  val supportedContainerTypes = Set(
    "Set",
    "Seq",
    "List",
    "Array",
    "Option",
    "Either",
    "Map"
  )

  def declareType[T, R <: ByteStreamReader[T], W <: ByteStreamWriter[T]] = macro declareType_impl[T, R, W]

  def declareType_impl[Z: c.WeakTypeTag, R: c.WeakTypeTag, W: c.WeakTypeTag](c: blackbox.Context) = {
    import c.universe._
    println("HELLO")
    val tpe = weakTypeOf[Z]
    val symbol = weakTypeOf[Z].typeSymbol

    def termTree(t: c.universe.Type): Term = {
      //println(s"t: ${t}")
      val symbol = t.typeSymbol //; println(s"symbol: ${symbol}")
      if( supportedContainerTypes.map(_+"[").exists(t.toString.startsWith) ){
        t match{
          case TypeRef(_, _, args) if args.size == 1 =>
            val tType = args.head
            MonoContainer(symbol.fullName, termTree(tType))
          case TypeRef(_, _, args) if args.size == 2 =>
            val tType = args.head
            val uType = args.drop(1).head
            BiContainer(symbol.fullName, termTree(tType), termTree(uType))
        }
      } else if(symbol.asClass.isTrait){
        Leaf(symbol.fullName)
      }else if(supportedBaseTypes.contains(symbol.fullName)){
        Leaf(symbol.fullName)
      }
      else{
        val constr: MethodSymbol = t.members.find {
          case constr: MethodSymbol if constr.isConstructor => true
          case _ => false
        }.get.asInstanceOf[MethodSymbol]
        val params = constr.asMethod.paramLists.head   //println(s"a.paramss: ${params}")
        Node(symbol.fullName,
          params.map {
            case x =>
              val dataType = x.info.toString  //println(s"dataType: ${dataType}")
              //println(s"${x} hasDefault: ${x.asTerm.isParamWithDefault} info: ${x.info}")
              (x.fullName -> Property(termTree(x.info), x.asTerm.isParamWithDefault) )
          }.toMap)
        }
    }
    val rootNode = termTree(tpe).asInstanceOf[Node]
    println(s"rootNode: ${rootNode}")


    //println(s"members: ${tpe.members}")
    val rootScope = scoped(rootNode, "")

    val root = Paths.get( this.getClass.getResource("/").getPath )
    // We want the resource path before compile
    val db = Paths.get( root.toString + "/../../../src/main/resources/typebus" )
    if(Files.notExists(db))
      Files.createDirectory(db)
    val typeTable = Paths.get(db + s"/${symbol.fullName}")

    if(Files.notExists(typeTable)){
      Files.write(typeTable, serialise(rootNode))
    }

    // compare the database type with the "rootNode" type.
    val dbNode = deSerialise(Files.readAllBytes(typeTable))
    val dbScoped = scoped(dbNode, "")

    val adds = rootScope.toSet diff dbScoped.toSet  //; println(s"\n\nadds: ${adds}\n")

    val subs = dbScoped.toSet diff rootScope.toSet  //; println(s"\n\nsubs: ${subs}\n")

    // A schema is invalid under the following conditions:
    // * adding a new field without a default value
    // * removing a field that does not contain a default value
    // * changing the type of a field

    val checkAdditions = collapseTree(adds.toList) //; println(s"\n\nadds: ${collapseTree(adds.toList) }\n")
    val checkSubtractions = collapseTree(subs.toList) //; println(s"\n\nsubs: ${collapseTree(subs.toList) }\n")

    // get a map of "field path" to type
    val addsTypeMap = checkAdditions.flatMap{ s => s.path.split(":").toList match{
      case f :: t :: Nil => Some(f -> t)
      case _ => None
    }}.toMap
    val subsTypeMap = checkSubtractions.flatMap{ s => s.path.split(":").toList match{
      case f :: t :: Nil => Some(f -> t)
      case _ => None
    }}.toMap

    val typeCheck = subsTypeMap.keys.flatMap{ k =>
      addsTypeMap.get(k).map{ t =>
        k -> (subsTypeMap(k),t)
      }
    }

    if(! typeCheck.isEmpty && !typeCheck.forall{ case (k, (t1, t2)) => t1 == t2 } ){ // changing the type of a field
      c.abort(c.enclosingPosition,
        s"""
           |Schema evolution Faild !!
           |You changed the type of one of you member fields
           |Type failed:
           |${typeCheck}
           |
        """.stripMargin)
    }else if(!checkAdditions.isEmpty && checkAdditions.exists(!_.hasDefault) ){ // adding a new field without a default value ?
      c.abort(c.enclosingPosition,
        s"""
           |Schema evolution Faild !!
           |You have added a new field to a typebus type that does not have a default value.
           |Type failed:
           |${checkAdditions.find(! _.hasDefault).get}
           |
        """.stripMargin)
    }else if(!checkSubtractions.isEmpty && checkSubtractions.exists(!_.hasDefault) ){ // removing a field that does not contain a default value ?
      c.abort(c.enclosingPosition,
        s"""
           |Schema evolution Faild !!
           |You have removed a field from your schema that did not contain a default value
           |Type failed:
           |${checkSubtractions.find(! _.hasDefault).get}
           |
        """.stripMargin)
    }else{
      // only get here if the checks pass...
      val updatedSchema = merge(rootNode, dbNode) // ;println(s"\n\nmerged:\n ${updatedSchema}\n\n")
      Files.write(typeTable, serialise(updatedSchema))
      // create a new class
      //https://stackoverflow.com/questions/29352611/instantiate-class-symbol-using-macro?rq=1
      val Wtpe = weakTypeOf[W]
      val Rtpe = weakTypeOf[R]
      val fqn = symbol.fullName
      val code =
        q"""
            io.surfkit.typebus.module.Service.registerServiceType(new $Rtpe, $fqn)
            new ByteStreamReaderWriter(new $Rtpe, new $Wtpe)
         """
      //println(showCode(code))
      c.Expr[ByteStreamReaderWriter[Z]](code)
    }
  }

  /***
    * Convert an AST Node type to a byte array.  Used for writing to our DB
    * @param value Node that we want to write
    * @return - bytes
    */
  def serialise(value: Node): Array[Byte] = {
    Pickle.intoBytes(value).array
  }

  /***
    * Read an AST Node type from an array of bytes.  Used fore reading from our DB
    * @param bytes
    * @return
    */
  def deSerialise(bytes: Array[Byte]): Node = {
    Unpickle[Node].fromBytes(java.nio.ByteBuffer.wrap(bytes))
  }

  /***
    * Given 2 Nodes we want to merge them into a final single Node
    * @param r - right Node to merge
    * @param l - left Node to merge
    * @return - The resulting merged Node
    */
  def merge(r: Node, l: Node): Node = {
    val mergeMap: Map[String, (Option[Property], Option[Property])] = (l.members.keys ++ r.members.keys).map( k => (k -> (l.members.get(k), r.members.get(k)) ) ).toMap
    val mergedMembers = mergeMap.map{
      case (k, (Some(Property(ln:Node, lHasDefault)), Some(Property(rn:Node, rHasDefault)))) =>
        k -> Property(merge(ln, rn), lHasDefault && rHasDefault)
      case (k, (Some(x), Some(y))) => k -> x
      case (k, (None, Some(y))) => k -> y
      case (k, (Some(x), None)) => k -> x
    }
    Node(r.`type`, mergedMembers)
  }

  /***
    * recursive function to produce a List of PropScope.  This is a flat list of fully qualified property names
    * @param t - Term
    * @param prefix - the prefrix is the current depth in the object heirarchy
    * @return - the list of PropScope
    */
  def scoped(t: Term, prefix: String): List[PropScope] = t match {
    case x: Leaf => Nil
    case x: Node =>
      x.members.flatMap( y => PropScope(prefix+"/"+y._1+":"+y._2.term.`type`, y._2.hasDefault) :: scoped(y._2.term, prefix + "/" + y._1) ).toList
    case x: MonoContainer =>
      PropScope(prefix+"/"+x.`type`+"|"+x.contains.`type`, false) ::
        scoped(x.contains, prefix + "/" + x.`type`)
    case x: BiContainer =>
      PropScope(prefix+"/"+x.`type`+"|"+x.left.`type`, false) ::
        PropScope(prefix+"/"+x.`type`+"|"+x.right.`type`, false) ::
        scoped(x.left, prefix + "/" + x.`type`) ::: scoped(x.right, prefix + "/" + x.`type`)
  }

  /***
    * Items with default values need to "shadow" out all their children
    * @param tree
    * @return - List[PropScope] with shadowed types removed
    */
  def collapseTree(tree: List[PropScope]): List[PropScope] =
    tree.sortBy(_.path).reverse.foldLeft( List.empty[PropScope] ){ (xs: List[PropScope], x: PropScope) =>
      //println(s"xs: ${xs}")
      xs.headOption match{
        case Some(head) if(x.path.startsWith(head.path.replace(":","/"))) => xs
        case _ => x :: xs
      }
    }
}
