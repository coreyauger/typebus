package io.surfkit.typebus

import java.io.FileNotFoundException
import java.nio.file.{FileSystems, Files, Path, Paths}


package object gen {

  /***
    * Gen - base type for code generator
    */
  sealed trait Gen{}

  /***
    * Fqn - wrap a Fully Qualified Name of a type.
    * @param id - the Fully Qualified Name of a type.
    */
  final case class Fqn(id: String) extends Gen

  /***
    * GeneratedClass - The scala code for an avro generated case class type
    * @param fqn - the Fully Qualified Name of a type.
    * @param packageName - Package this cases class resides in.
    * @param simpleName - The Simple Name for the case class
    * @param caseClassRep - The "scala/typescript/python" source code for this type
    */
  final case class GeneratedClass(
                                 fqn: Fqn,
                                 packageName: String,
                                 simpleName: String,
                                 classRep: String
                               ) extends Gen

  /***
    * ServiceMethodGenerator - defines a service level function.
    * @param in - The IN type for the service function
    * @param out - The OUT type for the service function
    */
  final case class ServiceMethodGenerator(
                                   in: Fqn,
                                   out: Fqn
                                   ) extends Gen

  /***
    * Language - type of lang you want code for.
    */
  sealed trait Language
  object Language{
    case object Scala extends Language
    case object Typescript extends Language
    case object Python extends Language
  }

  /***
    * ServiceGenerator - Store information needed to generate types and RPC client to a service.
    * @param serviceName - The name of the service
    * @param methods - All the service level methods that have been declared.
    * @param caseClasses - Scala case class generated for all the types required.
    */
  final case class ServiceGenerator(
                             serviceName: String,
                             language: Language,
                             methods: Set[ServiceMethodGenerator],
                             classes: Set[GeneratedClass]
                             ) extends Gen


  /***
    * ScalaCodeWriter - Object that does all the code gen
    */
  object ScalaCodeWriter{
    def serviceToClassName(serviceName: String) =
      serviceName.split("-").filterNot(_.isEmpty).map(x => x.head.toUpper + x.drop(1)).mkString

    /***
      * writeService - write the source code needed to generate a service with types and RPC client.
      * @param generator - ServiceGenerator definition
      * @return - List of tuple containing (package name, source code).
      */
    def writeService(generator: ServiceGenerator): List[(String, String)] = {
      val methodMap = generator.methods.map(x => x.in -> x.out).toMap
      val fqlToCaseClass = generator.classes.map(x => x.fqn -> x).toMap
      generator.classes.groupBy(_.packageName).map{
        case (packageName, classes) =>
          val sb = new StringBuffer()
          sb.append("/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */\n\n")
          sb.append(s"package ${packageName.split('.').reverse.drop(1).reverse.mkString(".")}\n\n")
          sb.append("import akka.actor.ActorSystem\n")
          sb.append("import scala.concurrent.Future\n")
          sb.append("import io.surfkit.typebus._\n")
          sb.append("import io.surfkit.typebus.event.EventMeta\n")
          sb.append("import io.surfkit.typebus.bus.Publisher\n")
          sb.append("import io.surfkit.typebus.client._\n")
          sb.append("import io.surfkit.typebus.event.{ServiceIdentifier, ServiceException}\n\n")
          sb.append(s"package object ${packageName.split('.').last}{\n\n")
          sb.append( classes.map(x => x.classRep).mkString("\n") )
          sb.append(s"\n\n   object Implicits extends AvroByteStreams{\n")
          sb.append( classes.map{ cc =>
            s"""
              |      implicit val ${cc.simpleName}Reader = new AvroByteStreamReader[${cc.simpleName}]
              |      implicit val ${cc.simpleName}Writer = new AvroByteStreamWriter[${cc.simpleName}]
            """.stripMargin
          }.mkString("") )
          sb.append(s"\n   }")
          // add the client mappings...
          sb.append("\n\n   /** Generated Actor Client */\n")
          // FIXME: we dont' know the service name when generating from the cli
          //sb.append(s"   class ${serviceToClassName(generator.serviceName)}Client(serviceIdentifier: ServiceIdentifier)(implicit system: ActorSystem) extends ${busType}Client(serviceIdentifier){\n")
          sb.append(s"   class RpcClient(serviceIdentifier: ServiceIdentifier, publisher: Publisher)(implicit system: ActorSystem) extends Client(serviceIdentifier, publisher, system){\n")
          sb.append( "      import Implicits._\n")
          val methodsInThisPackage = classes.flatMap(x => methodMap.get(x.fqn).map{ y => ServiceMethodGenerator(x.fqn, y) } )
          //println(s"MAP: \n\n\n${fqlToCaseClass}\n\n")
          sb.append( methodsInThisPackage.map{ method =>
            try{
              val inType = fqlToCaseClass(method.in)
              val outType = fqlToCaseClass(method.out)
            }catch{
              case t: Throwable =>
                println("\n\nfqlToCaseClass MAP ******************************")
                println(fqlToCaseClass)
                println("\n\n")
            }
            val inType = fqlToCaseClass(method.in)
            val outType = fqlToCaseClass(method.out)
            s"      def ${inType.simpleName.take(1).toLowerCase}${inType.simpleName.drop(1)}(x: ${inType.simpleName}, eventMeta: Option[EventMeta] = None): Future[Either[ServiceException,${outType.simpleName}]] = wire[${inType.simpleName}, ${outType.simpleName}](x, eventMeta)"
          }.mkString("\n") )
          sb.append(s"\n   }")

          sb.append(s"\n}\n")
          (packageName, sb.toString)
      }.toList
    }

    /***
      * writeCodeToFiles - writes the source code to the project directory to be compiled
      * @param generator - ServiceGenerator definition
      */
    def writeCodeToFiles(generator: ServiceGenerator, basePath: List[String] = List("src", "main", "scala")) = {
      try {
        writeService(generator).foreach {
          case (packageName, sourceCode) =>
            val path = (basePath ::: packageName.split('.').toList).toArray
            val modelPath = Paths.get(path.mkString("/"))
            if (!Files.exists(modelPath))
              Files.createDirectories(modelPath)
            val filePath = Paths.get(path.mkString("/") + "/data.scala")
            Files.write(filePath, sourceCode.getBytes)
        }
      }catch{
        case t:Throwable =>
          println(s"ERROR: ${t.getMessage}")
          t.printStackTrace()
      }

    }
  }




  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  import io.surfkit.typebus.Typebus._
  import io.surfkit.typebus.annotations.ServiceMethod._

  val prefix = "api."

  sealed trait Typed{ def pos: Int }
  case class Simple(name: String, pos: Int) extends Typed{
    override def toString: String = name
  }
  case class MonoTyped(container: String, `type`: String, pos: Int) extends Typed{
    override def toString: String = s"${container}[${`type`}]"
  }
  case class BiTyped(container: String, left: String, right: String, pos: Int) extends Typed{
    override def toString: String = s"${container}[${left},${right}]"
  }

  case class CodeSrc(symbol: io.surfkit.typebus.Typebus.Symbol, `type`: String, members: Map[String, (Typed, Option[String])], baseClasses: Set[String])

  def tree2CodeSrc(trees: Seq[Node]): List[(String, CodeSrc)] = {
    def shortName(name: String) = name.split('.').last
    def collapseNode(n: Node): List[CodeSrc] = {
      val (members: List[(String, (Typed, Option[String]))], nodes: Iterable[List[Node]]) = n.members.map{
        case (name, Property(Leaf(t), p, d, dv)) => (shortName(name),(Simple(t, p), dv), List.empty[Node])
        case (name, Property(n @ Node(_,t, _, _, _), p, d, dv)) => (shortName(name),(Simple(t, p), dv), List(n))
        case (name, Property(MonoContainer(c, t), p, d, dv)) => (shortName(name),(MonoTyped(c, t.`type`, p), dv), if(t.isInstanceOf[Node]) List(t.asInstanceOf[Node]) else List.empty[Node])
        case (name, Property(BiContainer(c, t1, t2), p, d, dv)) => (shortName(name), (BiTyped(c,t1.`type`,t2.`type`, p), dv), List[Option[Node]]( if(t1.isInstanceOf[Node]) Some(t1.asInstanceOf[Node]) else None, if(t2.isInstanceOf[Node]) Some(t2.asInstanceOf[Node]) else None).flatten)
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
    // collect a list of all the fqn names that we will generate on..
    // we want to prefix all our codegen namespace behind "api."
    val fqnSet = codes.map(_._2.`type`).toSet
    def prefixName(name: String) =
      if(fqnSet.contains(name))prefix + name
      else name
    def prefixTyped(typed: Typed) = typed match{
      case Simple(name, p) => Simple(prefixName(name), p)
      case MonoTyped(c, t, p) => MonoTyped(prefixName(c), prefixName(t), p)
      case BiTyped(c, l, r, p) => BiTyped(prefixName(c), prefixName(l), prefixName(r), p)
    }
    val prefixedCodes = codes.map{
      case (pack, codeSrc) => (prefix+pack, CodeSrc(
        symbol = codeSrc.symbol,
        `type` = prefixName(codeSrc.`type`),
        members = codeSrc.members.map{
          case (name, (t, default)) => name -> (prefixTyped(t), default)
        },
        baseClasses = codeSrc.baseClasses.map(prefixName)
      ))
    }
    val grouped = prefixedCodes.groupBy(x => x._1.split('.').reverse.drop(1).reverse.mkString("."))
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
                classRep =s"   final case class ${typeToken}(${code.members.toList.sortBy(_._2._1.pos).map(x => s"${x._1}: ${x._2._1}${x._2._2.map(y => s" = ${y}").getOrElse("")}").mkString(", ")})${inheritenceStr}"))
            case Symbol.Trait =>
              Some(gen.GeneratedClass(
                fqn = gen.Fqn(code.`type`),
                packageName = pack,
                simpleName = typeToken,
                classRep =s"   sealed trait ${typeToken}${inheritenceStr}{\n${code.members.toList.sortBy(_._2._1.pos).map(x => s"      def ${x._1}: ${x._2._1}${x._2._2.map(y => s" = ${y}").getOrElse("")}").mkString("\n      ")}\n   }"))
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
                  s"      final case class ${tt}(${y.members.toList.sortBy(_._2._1.pos).map(x => s"${x._1}: ${x._2._1}${x._2._2.map(y => s" = ${y}").getOrElse("")}").mkString(", ")})${inheritenceStr2}"
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
    //println(s"path: ${database}")
    if(Files.isDirectory(database)) {
      val typebusDb =
        if (database.endsWith("typebus"))database
        else database.resolve(Paths.get("src/main/resources/typebus/"))
      //println(s"resolved: ${typebusDb}")
      if(Files.isDirectory(typebusDb)) {
        val walk = Files.walk(typebusDb, 1)
        var astTree = List.empty[Node]
        var serviceStore: ServiceStore = null
        val it=walk.iterator()
        it.next()  // ignore "/typebus" directory we only want the contents.
        it.forEachRemaining{ x =>
          if(x.endsWith("_Service"))serviceStore = deSerialiseServiceStore(Files.readAllBytes(x))
          else astTree = deSerialise(Files.readAllBytes(x)) :: astTree
        }
        astNodeToServiceGenerator(astTree, serviceStore.methods)
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
    var serviceStore: ServiceStore = null
    val it=walk.iterator()
    it.next()  // ignore "/typebus" directory we only want the contents.
    it.forEachRemaining{ x =>
      if(x.endsWith("_Service"))serviceStore = deSerialiseServiceStore(Files.readAllBytes(x))
      else astTree = deSerialise(Files.readAllBytes(x)) :: astTree
    }
    fileSystem.close()
    astNodeToServiceGenerator(astTree, serviceStore.methods)
  }

  def astNodeToServiceGenerator(astTree: List[Node], methods: Set[ServiceMethod])={
    val srcList = tree2CodeSrc(astTree)
    //srcList.foreach(println)
    val generated = srcCodeGenerator(srcList)
    //println(s"\n\n BLARG\n:${methods}")
    val serviceGenerator = gen.ServiceGenerator(
      "service-name",
      gen.Language.Scala,
      methods = methods.map( x => ServiceMethodGenerator(Fqn(prefix + x.in), Fqn(prefix + x.out)) ),
      classes = generated.flatMap(_._2).toSet
    )
    //println(serviceGenerator)

    serviceGenerator
  }
}
