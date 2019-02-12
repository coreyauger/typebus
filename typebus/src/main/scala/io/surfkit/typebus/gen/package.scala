package io.surfkit.typebus

import java.nio.file.{Files, Paths}


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
                             methods: Seq[ServiceMethodGenerator],
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
    def writeService(busType: String, generator: ServiceGenerator): List[(String, String)] = {
      val methodMap = generator.methods.map(x => x.in -> x.out).toMap
      val fqlToCaseClass = generator.classes.map(x => x.fqn -> x).toMap
      generator.classes.groupBy(_.packageName).map{
        case (packageName, classes) =>
          val sb = new StringBuffer()
          sb.append("/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */\n\n")
          sb.append(s"package ${packageName.split('.').reverse.drop(1).reverse.mkString(".")}\n\n")
          sb.append("import akka.actor.ActorSystem\n")
          sb.append("import io.surfkit.typebus.Implicits._\n")
          sb.append("import scala.concurrent.Future\n")
          sb.append("import io.surfkit.typebus._\n")
          sb.append("import io.surfkit.typebus.event.EventMeta\n")
          sb.append("import io.surfkit.typebus.client._\n")
          sb.append("import io.surfkit.typebus.event.ServiceIdentifier\n\n")
          sb.append(s"package object ${packageName.split('.').last}{\n\n")
          sb.append( classes.map(x => "  "+ x.classRep).mkString("\n") )
          sb.append(s"\n\n  object Implicits extends AvroByteStreams{\n")
          sb.append( classes.map{ cc =>
            s"""
              |   implicit val ${cc.simpleName}Reader = new AvroByteStreamReader[${cc.simpleName}]
              |   implicit val ${cc.simpleName}Writer = new AvroByteStreamWriter[${cc.simpleName}]
            """.stripMargin
          }.mkString("") )
          sb.append(s"\n  }")
          // add the client mappings...
          sb.append("\n\n  /** Generated Actor Client */\n")

          sb.append(s"  class ${serviceToClassName(generator.serviceName)}Client(serviceIdentifier: ServiceIdentifier)(implicit system: ActorSystem) extends ${busType}Client(serviceIdentifier){\n")
          sb.append( "    import Implicits._\n")
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
            s"     def ${inType.simpleName.take(1).toLowerCase}${inType.simpleName.drop(1)}(x: ${inType.simpleName}, eventMeta: Option[EventMeta] = None): Future[${outType.simpleName}] = wire[${inType.simpleName}, ${outType.simpleName}](x, eventMeta)"
          }.mkString("\n") )
          sb.append(s"\n  }")

          sb.append(s"\n}\n")
          (packageName, sb.toString)
      }.toList
    }

    /***
      * writeCodeToFiles - writes the source code to the project directory to be compiled
      * @param generator - ServiceGenerator definition
      */
    def writeCodeToFiles(busType: String, generator: ServiceGenerator, basePath: List[String] = List("src", "main", "scala")) = {
      try {
        writeService(busType, generator).foreach {
          case (packageName, sourceCode) =>
            val path = (basePath ::: packageName.split('.').toList).toArray
            val modelPath = Paths.get(path.mkString("/"))
            if (!Files.exists(modelPath))
              Files.createDirectories(modelPath)
            val filePath = Paths.get(path.mkString("/") + "/data.scala")
            //if(!Files.exists(filePath)){
            Files.write(filePath, sourceCode.getBytes)
        }
      }catch{
        case t:Throwable =>
          println(s"ERROR: ${t.getMessage}")
          t.printStackTrace()
      }

    }
  }

}
