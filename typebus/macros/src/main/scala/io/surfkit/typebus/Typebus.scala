package io.surfkit.typebus

import java.nio.file.{FileSystems, Files, Path, Paths}
import java.io._
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

import scala.collection.mutable
import scala.concurrent.Future
import io.surfkit.typebus.event.EventMeta
import com.typesafe.config.ConfigFactory
import play.api.libs.json._

import scala.collection.JavaConversions._
import scala.util.Try

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


object Typebus extends ResourceDb{
  import scala.language.experimental.macros
  import scala.reflect.macros.blackbox

  val config = ConfigFactory.load()

  /***
    * Helper types to create a AST of only the parts we care about
    */
  sealed trait Term{
    def `type`: String
  }
  object Term{
    implicit val format: OFormat[Term] = Json.format[Term]
  }

  /***
    * This type represents the AST type for a case class property
    * @param term - The AST type for the propery
    * @param pos - The position the term was found in
    * @param hasDefault - if there as a default value provided or not.
    */
  case class Property(term: Term, pos: Int, hasDefault: Boolean, defaultValue: Option[String] = None)
  object Property{
    implicit val format: Format[Property] = Json.format[Property]
  }

  /***
    * Terminator Node that contains a Type
    * @param `type` - the type for this terminator
    */
  case class Leaf(`type`: String) extends Term
  object Leaf{
    implicit val format: Format[Leaf] = Json.format[Leaf]
  }

  sealed trait Symbol
  object Symbol{
    case object CaseClass extends Symbol
    case object Trait extends Symbol
    case object Companion extends Symbol
    case object CompanionCaseClass extends Symbol

    // create the formats and provide them implicitly
    implicit object format extends Format[Symbol] {
      def reads(json: JsValue): JsResult[Symbol] = (json \ "Symbol").validate[String].map(_ match{
        case "CaseClass" => CaseClass
        case "Trait" => Trait
        case "Companion" => Companion
        case "CompanionCaseClass" => CompanionCaseClass
      })

      def writes(x: Symbol): JsValue = Json.obj(
        "Symbol" -> (x match{
          case CaseClass => "CaseClass"
          case Trait => "Trait"
          case Companion => "Companion"
          case CompanionCaseClass => "CompanionCaseClass"
        })
      )
    }
  }

  /***
    * This is a Complext type (class, case class)
    * @param `type` - the type of the case class
    * @param members - all the members declared in the case class (mapping of "name" -> Property)
    */
  case class Node(symbol: Symbol, `type`: String, members: Map[String, Property], baseClasses: Seq[Term], companion: Option[Node]) extends Term
  object Node{
    implicit val format: Format[Node] = Json.format[Node]
  }


  /***
    * Type containers with a single Type parameter, eg: Option, Seq, ...
    * @param `type` - The type of container
    * @param contains - The type "inside" the container.
    */
  case class MonoContainer(`type`: String, contains: Term) extends Term
  object MonoContainer{
    implicit val format: Format[MonoContainer] = Json.format[MonoContainer]
  }

  /***
    * Type container with 2 Type parameters, eg: Either, Map
    * @param `type` - the type of the container
    * @param left - left "contained" type
    * @param right - right "contained" type
    */
  case class BiContainer(`type`: String, left: Term, right: Term) extends Term
  object BiContainer{
    implicit val format: Format[BiContainer] = Json.format[BiContainer]
  }

  /*****************************************************************************************************************/

  /***
    * Helper class to "flatten" and object into scoped paths
    * @param path - fully qualified name path
    * @param hasDefault - if this contains a default value
    */
  case class PropScope(path: String, hasDefault: Boolean)
  object PropScope{
    implicit val format: Format[PropScope] = Json.format[PropScope]
  }

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
    "java.time.Instant",
    "java.util.UUID",
    "Serializable",
    "java.io.Serializable",
    "scala.Serializable",
    "scala.Product",
    "scala.Equals",
    "java.lang.Object",
    "scala.Object",
    "scala.Any"

  ) ++ Try(config.getStringList("bus.code-gen.base-types").toSet).toOption.getOrElse(Set.empty[String])

  val supportedContainerTypes = Set(
    "Set",
    "Seq",
    "List",
    "Array",
    "Option",
    "Either",
    "Map"
  )

  val startBraces = Set('(','[','{')
  val endBraces = Set(')',']','}')


  def declareType[T, R <: ByteStreamReader[T], W <: ByteStreamWriter[T]] = macro declareType_impl[T, R, W]

  def declareType_impl[Z: c.WeakTypeTag, R: c.WeakTypeTag, W: c.WeakTypeTag](c: blackbox.Context) = {
    import c.universe._
    val tpe = weakTypeOf[Z]
    val symbol = weakTypeOf[Z].typeSymbol

    def extractDefaultParam(src: String, caseClass: String, symbol:  c.universe.Symbol): String = {
      val cc = caseClass.split('.').last
      val prop = symbol.fullName.split('.').last
      val start= s"(?s)case\\s+class\\s+${cc}\\s*\\(".r.findFirstMatchIn(src).map(_.start).get
      //println(s"start: ${start}")
      val strRest = src.substring(start-1)
      val s = strRest.substring(strRest.indexOf('(')+1)
      //println(s"s: ${s.take(20)}")
      val parms = s.foldLeft( (0, List.empty[Char]) ){ case ((par, str), ch) =>
        if(par < 0) (par, str)
        else if(ch == ')' && par == 0)(-1, str)  // don't include the last ')'
        else (if(ch == '(') par+1 else if(ch == ')' ) par-1 else par, ch :: str)
      }._2.mkString.reverse.replaceAll("\\n", "")
      //println(s"\nmatches: ${parms}")
      val located = parms.foldLeft[(Int, Int, Int, Boolean, List[String])]( (0, 0, 0, false, List.empty[String]) ){ case ((start, pos, braceCount, inLiteral, xs: List[String]), ch) =>
        //print(ch)
        if(ch == ',' && braceCount == 0 && !inLiteral){
          (pos+1, pos+1, braceCount, inLiteral, parms.substring(start, pos) :: xs)
        }else if(pos == parms.length-1){
          (pos+1, pos+1, braceCount, inLiteral, parms.substring(start) :: xs)
        }else{
          (start, pos+1, if(startBraces.contains(ch))braceCount+1 else if(endBraces.contains(ch))braceCount-1 else braceCount, if(ch == '"') !inLiteral else inLiteral,  xs)
        }
      }._5.map(_.trim)
      //println(s"\nlocated: ${located}")
      //println(s"prop: ${prop}")
      located.filter(_.trim.startsWith(prop)).headOption match{
        case Some(eq) if eq.contains('=') => eq.split('=').last.trim
        case _ => c.abort(c.enclosingPosition, s"Could not parse default value for type: ${cc}.${prop} failed to parse: ${located} from: ${caseClass}, sym: ${symbol}")
      }
    }


    def termTree(t: c.universe.Type, exploreCompanion: Boolean = true): Term = {
      //println(s"t: ${t}")
      var pos = new AtomicInteger(0)
      val symbol = t.typeSymbol ; //println(s"identify symbol: ${symbol}")
      if( supportedContainerTypes.map(_+"[").exists(t.toString.startsWith) ){
        t match{
          case TypeRef(_, _, args) if args.size == 1 =>
            val tType = args.head
            MonoContainer(symbol.fullName, termTree(tType))
          case TypeRef(_, _, args) if args.size == 2 =>
            val tType = args.head
            val uType = args.drop(1).head
            BiContainer(symbol.fullName, termTree(tType, exploreCompanion), termTree(uType, exploreCompanion))
        }
      }else if(supportedBaseTypes.contains(symbol.fullName)) {
        Leaf(symbol.fullName)
      } else if(symbol.asClass.isTrait){
        //println(s"Trait: ${symbol.fullName}")
        //println(s"t: ${t}")
        val methods = try {
          t.members.filter {
            case x: MethodSymbol if x.isAbstract => true
            case _ => false
          }.toSeq.asInstanceOf[Seq[MethodSymbol]]
        }catch{
          case _: Throwable => Seq.empty[MethodSymbol]
        }
        val baseClasses = try{
          t.baseClasses.map(x => t.baseType(x)).filterNot(_ == t)
            .filterNot(x => supportedBaseTypes.contains(x.typeSymbol.fullName))
            .map(termTree(_,exploreCompanion))
        }catch{
          case _: Throwable => Seq.empty[Term]
        }
        //println(s"methods: ${methods}")
        //println(s"baseClasses: ${baseClasses}")
        //println(s"[${symbol.fullName}] BASE CLASSES: ${t.baseClasses.map(x => t.baseType(x))}")
       // println(s"Companion: ${t.companion}")
        val comp = if(t.companion != NoType && exploreCompanion){
            val c = Node(
              symbol = Symbol.Companion,
              `type` = t.companion.typeSymbol.fullName,
              members = t.companion.members.filter {
                case x: TermSymbol if x.toString.startsWith("object") => true
                case _ => false
              }.map {
                case x =>
                  //val dataType = x.info.toString  //println(s"dataType: ${dataType}")
                  //println(s"${x} hasDefault: ${x.asTerm.isParamWithDefault} info: ${x.info}")
                  (x.fullName -> Property(termTree(x.info, false), pos.getAndIncrement(), x.asTerm.isParamWithDefault) )
              }.toMap,
              baseClasses = Seq.empty[Term],
              companion = None
            )
            Some(c)
          } else None

        Node(
          symbol = Symbol.Trait,
          `type` = symbol.fullName,
          members = methods.map {
            case x =>
              val dataType = x.info.toString  //println(s"dataType: ${dataType}")
              //println(s"${x} hasDefault: ${x.asTerm.isParamWithDefault} info: ${x.info}")
              (x.fullName -> Property(termTree(x.info, exploreCompanion), pos.getAndIncrement(), x.asTerm.isParamWithDefault) )
          }.toMap,
          baseClasses = baseClasses,
          companion = comp
        )
      }else {
        val constrOpt: Option[MethodSymbol] = t.members.find {
          case x: MethodSymbol if x.isConstructor => true
          case _ => false
        }.headOption.asInstanceOf[Option[MethodSymbol]]
        if (constrOpt.isEmpty) {
          Leaf(symbol.fullName)
        } else {
          val constr = constrOpt.get
          //println(s"[${symbol.fullName}] BASE CLASSES: ${t.baseClasses.map(x => t.baseType(x))}")
          val params = constr.asMethod.paramLists.head //println(s"a.paramss: ${params}")
          Node(
            symbol = if(exploreCompanion) Symbol.CaseClass else Symbol.CompanionCaseClass,
            `type` = symbol.fullName,
            members = params.zipWithIndex.map {
              case (x,i) =>
                val dataType = x.info.toString //println(s"dataType: ${dataType}")
                val defaultValueOpt = if(x.asTerm.isParamWithDefault){
                  //val defaultName = x.fullName.split('.').last+"$default$"+(i+1)
                  val moduleSym = symbol.companion

                  val defaultName = symbol.fullName+".apply$default$"+(i+1)
                  moduleSym.typeSignature.members.collect{
                    case _ => // CA - this seems required to expand the method names  (need to collect them?)
                  }
                  val defaultExprOpt = moduleSym.typeSignature.members.find(_.fullName == defaultName)
                  //println(s"defaultExprOpt: ${defaultExprOpt}")
                  defaultExprOpt.map{ exp =>
                    val src = c.enclosingPosition.source.content.mkString("")
                    extractDefaultParam(src, symbol.fullName, x)
                  }
                }else None
                //println(s"defaultValueOpt: ${defaultValueOpt}")
                //println(s"${x} hasDefault: ${x.asTerm.isParamWithDefault} info: ${x.info}")
                (x.fullName -> Property(termTree(x.info, exploreCompanion), pos.getAndIncrement(), x.asTerm.isParamWithDefault, defaultValueOpt))
            }.toMap,
            baseClasses = t.baseClasses.map(x => t.baseType(x)).filterNot(_ == t).map(termTree(_, exploreCompanion)).filterNot(x => supportedBaseTypes.contains(x.`type`)),
            companion = None // if(t.companion == NoType) None else Some(termTree(t.companion))
          )
        }
      }
    }
    val rootNode = termTree(tpe) match{
      case x: Node => x
      case x: BiContainer => Node(
          symbol = Typebus.Symbol.CaseClass,
          `type` = "io.surfkit.TypeWrapper",
          members = Map(
            "wrap" -> Property(x, 0, false)
          ),
          baseClasses = Seq.empty,
          companion = None
        )
      case _ => c.abort(c.enclosingPosition, "Bad root type for typebus mapping")
    }
    //println(s"rootNode: ${rootNode}")

    //println(s"members: ${tpe.members}")
    val rootScope = scoped(rootNode, "")
    // FIXME: packaging a jar will fail when trying to access the resource files.  For now we can skip this.
    try{
      val typeTable = databaseTablePath(symbol.fullName)
      if(Files.notExists(typeTable)){
        Files.write(typeTable, serialise(rootNode).getBytes)
      }

      // compare the database type with the "rootNode" type.
      val dbNode = deSerialise(Files.readAllLines(typeTable).mkString("\n"))
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

      val typeCheck = subsTypeMap.keys.flatMap{ k => addsTypeMap.get(k).map{ t => k -> (subsTypeMap(k),t) } }

      if(! typeCheck.isEmpty && !typeCheck.forall{ case (k, (t1, t2)) => t1 == t2 } ){ // changing the type of a field
        c.abort(c.enclosingPosition,
          s"""
             |Schema evolution Failed !!
             |You changed the type of one of you member fields
             |Type failed:
             |${typeCheck}
             |
          """.stripMargin)
      }else if(!checkAdditions.isEmpty && checkAdditions.exists(!_.hasDefault) ){ // adding a new field without a default value ?
        c.abort(c.enclosingPosition,
          s"""
             |Schema evolution Failed !!
             |You have added a new field to a typebus type that does not have a default value.
             |Type failed:
             |${Json.prettyPrint(Json.toJson(checkAdditions.find(! _.hasDefault).get))}
             |
          """.stripMargin)
      }else if(!checkSubtractions.isEmpty && checkSubtractions.exists(!_.hasDefault) ){ // removing a field that does not contain a default value ?
        c.abort(c.enclosingPosition,
          s"""
             |Schema evolution Failed !!
             |You have removed a field from your schema that did not contain a default value
             |Type failed:
             |${Json.prettyPrint(Json.toJson(checkSubtractions.find(! _.hasDefault).get))}
             |
          """.stripMargin)
      }else{
        // only get here if the checks pass...
        val updatedSchema = merge(rootNode, dbNode) // ;println(s"\n\nmerged:\n ${updatedSchema}\n\n")
        Files.write(typeTable, serialise(updatedSchema).getBytes)
        // create a new class
        //https://stackoverflow.com/questions/29352611/instantiate-class-symbol-using-macro?rq=1
        val Wtpe = weakTypeOf[W]
        val Rtpe = weakTypeOf[R]
        val fqn = symbol.fullName
        //println(s"\nzert: ${Wtpe}\nio.surfkit.typebus.module.Service.registerServiceType[${tpe}](new $Rtpe, $fqn)")
        val code =
          q"""
              io.surfkit.typebus.module.Service.registerServiceType[${tpe}](new $Rtpe, $fqn)
              new ByteStreamReaderWriter(new $Rtpe, new $Wtpe)
           """
        //println(showCode(code))
        c.Expr[ByteStreamReaderWriter[Z]](code)
      }
    }catch{
      case a: scala.reflect.macros.runtime.AbortMacroException => throw a // we want to fail on these :)
      case t: Throwable =>
        val Wtpe = weakTypeOf[W]
        val Rtpe = weakTypeOf[R]
        val fqn = symbol.fullName
        //println(s"\nzert: ${Wtpe}\nio.surfkit.typebus.module.Service.registerServiceType[${tpe}](new $Rtpe, $fqn)")
        val code =
          q"""
            io.surfkit.typebus.module.Service.registerServiceType[${tpe}](new $Rtpe, $fqn)
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
  def serialise(value: Node): String = Json.prettyPrint(Json.toJson(value))
    //Pickle.intoBytes(value).array

  /***
    * Read an AST Node type from an array of bytes.  Used fore reading from our DB
    * @param bytes
    * @return
    */
  def deSerialise(json: String): Node = Json.parse(json).as[Node]
    //Unpickle[Node].fromBytes(java.nio.ByteBuffer.wrap(bytes))

  /***
    * Given 2 Nodes we want to merge them into a final single Node
    * @param r - right Node to merge
    * @param l - left Node to merge
    * @return - The resulting merged Node
    */
  def merge(r: Node, l: Node): Node = {
    val mergeMap: Map[String, (Option[Property], Option[Property])] = (l.members.keys ++ r.members.keys).map( k => (k -> (l.members.get(k), r.members.get(k)) ) ).toMap
    val mergedMembers = mergeMap.map{
      case (k, (Some(Property(ln:Node, lpos, lHasDefault, lDefaultVal)), Some(Property(rn:Node, rpos, rHasDefault, _)))) =>
        k -> Property(merge(ln, rn), lpos, lHasDefault && rHasDefault, lDefaultVal)
      case (k, (Some(x), Some(y))) => k -> x
      case (k, (None, Some(y))) => k -> y
      case (k, (Some(x), None)) => k -> x
    }
    val mergeBaseClasses = (l.baseClasses ++ r.baseClasses).toSet.toSeq
    // What about someone that converts a trait to case class or the other way around?
    Node(r.symbol, r.`type`, mergedMembers, mergeBaseClasses, l.companion)
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
