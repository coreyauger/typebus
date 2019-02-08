package io.surfkit.typebus

import java.nio.file.{FileSystems, Files, Path, Paths}
import java.io._
import java.util.stream.Collectors

import boopickle.Default._

import scala.collection.mutable

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
  case class Property(term: Term, hasDefault: Boolean, defaultValue: Option[String] = None)

  /***
    * Terminator Node that contains a Type
    * @param `type` - the type for this terminator
    */
  case class Leaf(`type`: String) extends Term

  sealed trait Symbol
  object Symbol{
    case object CaseClass extends Symbol
    case object Trait extends Symbol
    case object Companion extends Symbol
    case object CompanionCaseClass extends Symbol
  }

  /***
    * This is a Complext type (class, case class)
    * @param `type` - the type of the case class
    * @param members - all the members declared in the case class (mapping of "name" -> Property)
    */
  case class Node(symbol: Symbol, `type`: String, members: Map[String, Property], baseClasses: Seq[Term], companion: Option[Node]) extends Term


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

  /*****************************************************************************************************************/

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

  ) // TODO: add the ability to pass in items to add to this Set

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
      val start= s"(?s)case\\s+class\\s+${cc}\\s*".r.findFirstMatchIn(src).map(_.start).get
      //println(s"start: ${start}")
      val strRest = src.substring(start)
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
        case _ => c.abort(c.enclosingPosition, s"Could not parse default value for type: ${cc}.${prop}")
      }
    }




    def termTree(t: c.universe.Type, exploreCompanion: Boolean = true): Term = {
      //println(s"t: ${t}")
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
                  (x.fullName -> Property(termTree(x.info, false), x.asTerm.isParamWithDefault) )
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
              (x.fullName -> Property(termTree(x.info, exploreCompanion), x.asTerm.isParamWithDefault) )
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
                (x.fullName -> Property(termTree(x.info, exploreCompanion), x.asTerm.isParamWithDefault, defaultValueOpt))
            }.toMap,
            baseClasses = t.baseClasses.map(x => t.baseType(x)).filterNot(_ == t).map(termTree(_, exploreCompanion)).filterNot(x => supportedBaseTypes.contains(x.`type`)),
            companion = None // if(t.companion == NoType) None else Some(termTree(t.companion))
          )
        }
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
           |${checkAdditions.find(! _.hasDefault).get}
           |
        """.stripMargin)
    }else if(!checkSubtractions.isEmpty && checkSubtractions.exists(!_.hasDefault) ){ // removing a field that does not contain a default value ?
      c.abort(c.enclosingPosition,
        s"""
           |Schema evolution Failed !!
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
            io.surfkit.typebus.module.Service.registerServiceType[${symbol}](new $Rtpe, $fqn)
            new ByteStreamReaderWriter(new $Rtpe, new $Wtpe)
         """
      //println(showCode(code))
      c.Expr[ByteStreamReaderWriter[Z]](code)
    }
  }

  case class CodeSrc(symbol: Symbol, `type`: String, members: Map[String, (String, Option[String])], baseClasses: Set[String])

  def tree2CodeSrc(trees: Seq[Node]): List[(String, CodeSrc)] = {
    def shortName(name: String) = name.split('.').last
    def collapseNode(n: Node): List[CodeSrc] = {
      val (members: List[(String, (String, Option[String]))], nodes: Iterable[List[Node]]) = n.members.map{
        case (name, Property(Leaf(t), d, dv)) => (shortName(name),(t, dv), List.empty[Node])
        case (name, Property(n @ Node(_,t, _, _, _), d, dv)) => (shortName(name),(t, dv), List(n))
        case (name, Property(MonoContainer(c, t), d, dv)) => (shortName(name),(s"${c}[${t.`type`}]", dv), if(t.isInstanceOf[Node]) List(t.asInstanceOf[Node]) else List.empty[Node])
        case (name, Property(BiContainer(c, t1, t2), d, dv)) => (shortName(name), (s"${c}[${t1.`type`}, ${t2.`type`}]", dv), List[Option[Node]]( if(t1.isInstanceOf[Node]) Some(t1.asInstanceOf[Node]) else None, if(t2.isInstanceOf[Node]) Some(t2.asInstanceOf[Node]) else None).flatten)
        case x => throw new RuntimeException(s"You have a Property that tyepbus does not know how to deal with.  This should not happen.  ${x}")
      }.map(x => (x._1 -> x._2, x._3) ).unzip

      CodeSrc(n.symbol, n.`type`, members.toMap, n.baseClasses.map(_.`type`).toSet) ::  n.baseClasses
          .filterNot(x => supportedBaseTypes.contains(x.`type`))
          .toList.asInstanceOf[List[Node]].map(collapseNode).flatten ::: nodes.toList.flatten.map(collapseNode).flatten ::: n.companion.map(collapseNode).getOrElse(List.empty[CodeSrc])
    }
    // want to collect all the Node types
    // sort them.. this should collect all the namespaces together
    trees.map(collapseNode).flatten.map( x => (x.`type` +"|"+ x.symbol, x) ).toSet.toList.sortBy[String](_._1)
  }

  def srcCodeGenerator(codes: List[(String, CodeSrc)]) = {
    val grouped = codes.groupBy(x => x._1.split('.').reverse.drop(1).reverse.mkString("."))
    grouped.map{
      case (pack, codeSrc) =>
        pack -> codeSrc.map(_._2).flatMap{ code =>
          val typeToken = code.`type`.split('.').last
          val inheritenceStr =
            if(code.baseClasses.isEmpty) ""
            else code.baseClasses.mkString(" extends "," with ","")
          code.symbol match{
            case Symbol.CaseClass if code.members.isEmpty =>
              Some(gen.GeneratedClass(
                fqn = gen.Fqn(code.`type`),
                packageName = pack,
                simpleName = typeToken,
                classRep = s"   final case object ${typeToken}${inheritenceStr}"))
            case Symbol.CaseClass =>
              Some(gen.GeneratedClass(
                fqn = gen.Fqn(code.`type`),
                packageName = pack,
                simpleName = typeToken,
                classRep =s"   final case class ${typeToken}(${code.members.map(x => s"${x._1}: ${x._2._1}${x._2._2.map(y => s" = ${y}").getOrElse("")}").mkString(", ")})${inheritenceStr}"))
            case Symbol.Trait =>
              Some(gen.GeneratedClass(
                fqn = gen.Fqn(code.`type`),
                packageName = pack,
                simpleName = typeToken,
                classRep =s"   sealed trait ${typeToken}${inheritenceStr}{\n${code.members.map(x => s"      def ${x._1}: ${x._2._1}${x._2._2.map(y => s" = ${y}").getOrElse("")}").mkString("\n      ")}\n   }"))
            case Symbol.Companion =>    // Uhg.. this is pretty cheesy..
              val inner = grouped.get(s"${pack}.${typeToken}").getOrElse( List.empty[(String, CodeSrc)]).map{ yy =>
                val y = yy._2
                val tt = y.`type`.split('.').last
                val inheritenceStr2 =
                  if(y.baseClasses.isEmpty) ""
                  else y.baseClasses.mkString(" extends "," with ","")
                if(y.members.isEmpty)
                  s"      final case object ${tt}${inheritenceStr2}\n"
                else
                s"      final case class ${tt}(${y.members.map(x => s"${x._1}: ${x._2._1}${x._2._2.map(y => s" = ${y}").getOrElse("")}").mkString(", ")})${inheritenceStr2}"
              }
              Some(gen.GeneratedClass(
                fqn = gen.Fqn(code.`type`),
                packageName = pack,
                simpleName = typeToken,
                classRep =s"   object ${typeToken}${inheritenceStr}{\n${inner.mkString}\n   }"))
            case _ => None
          }
        }
    }.filterNot(_._2.isEmpty)
  }

  def codeGen(database: Path) = {
    println(s"path: ${database}")
    if(Files.isDirectory(database)) {
      val typebusDb =
          if (database.endsWith("typebus"))database
          else database.resolve(Paths.get("src/main/resources/typebus/"))
      println(s"resolved: ${typebusDb}")
      if(Files.isDirectory(typebusDb)) {
        val walk = Files.walk(typebusDb, 1)
        var astTree = List.empty[Node]
        val it=walk.iterator()
        it.next()  // ignore "/typebus" directory we only want the contents.
        it.forEachRemaining{ x =>
          astTree = deSerialise(Files.readAllBytes(x)) :: astTree
        }
        astNodeToServiceGenerator(astTree)
      }else throw new FileNotFoundException(s"Not a typebus database location: ${database}")
    }else throw new FileNotFoundException(s"Not a directory: ${database}")

  }

  def selfCodeGen = {
    import scala.collection.JavaConverters._
    val uri = this.getClass.getResource("/typebus").toURI()
    val fileSystem = FileSystems.newFileSystem(uri, scala.collection.mutable.HashMap.empty[String, String].asJava)
    val tbPath =fileSystem.getPath("/typebus")
    val walk = Files.walk(tbPath, 1)
    var astTree = List.empty[Node]
    val it=walk.iterator()
    it.next()  // ignore "/typebus" directory we only want the contents.
    it.forEachRemaining{ x =>
      println(x)
      astTree = deSerialise(Files.readAllBytes(x)) :: astTree
    }
    fileSystem.close()
    astNodeToServiceGenerator(astTree)
  }

  def astNodeToServiceGenerator(astTree: List[Node])={
    val srcList = tree2CodeSrc(astTree)
    srcList.foreach(println)
    val generated = srcCodeGenerator(srcList)
    val serviceGenerator = gen.ServiceGenerator(
      "service-name",
      gen.Language.Scala,
      methods = Seq.empty,
      classes = generated.flatMap(_._2).toSet
    )
    println(serviceGenerator)

    serviceGenerator
  }

  /***
    * Convert an AST Node type to a byte array.  Used for writing to our DB
    * @param value Node that we want to write
    * @return - bytes
    */
  def serialise(value: Node): Array[Byte] =
    Pickle.intoBytes(value).array

  /***
    * Read an AST Node type from an array of bytes.  Used fore reading from our DB
    * @param bytes
    * @return
    */
  def deSerialise(bytes: Array[Byte]): Node =
    Unpickle[Node].fromBytes(java.nio.ByteBuffer.wrap(bytes))

  /***
    * Given 2 Nodes we want to merge them into a final single Node
    * @param r - right Node to merge
    * @param l - left Node to merge
    * @return - The resulting merged Node
    */
  def merge(r: Node, l: Node): Node = {
    val mergeMap: Map[String, (Option[Property], Option[Property])] = (l.members.keys ++ r.members.keys).map( k => (k -> (l.members.get(k), r.members.get(k)) ) ).toMap
    val mergedMembers = mergeMap.map{
      case (k, (Some(Property(ln:Node, lHasDefault, lDefaultVal)), Some(Property(rn:Node, rHasDefault, _)))) =>
        k -> Property(merge(ln, rn), lHasDefault && rHasDefault, lDefaultVal)
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
