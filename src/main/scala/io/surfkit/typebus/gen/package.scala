package io.surfkit.typebus

import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption._
import java.nio.file.{Files, Paths}

package object gen {

  sealed trait Gen{}

  final case class Fqn(id: String) extends Gen

  final case class GeneratedCaseClass(
                                 fqn: Fqn,
                                 packageName: String,
                                 simpleName: String,
                                 caseClassRep: String
                               ) extends Gen

  final case class ServiceMethodGenerator(
                                   in: Fqn,
                                   out: Fqn
                                   ) extends Gen

  final case class ServiceGenerator(
                             serviceName: String,
                             methods: Seq[ServiceMethodGenerator],
                             caseClasses: Set[GeneratedCaseClass]
                             ) extends Gen


  object ScalaCodeWriter{
    def writeService(generator: ServiceGenerator): List[(String, String)] = {
      val methodMap = generator.methods.map(x => x.in -> x.out).toMap
      generator.caseClasses.groupBy(_.packageName).map{
        case (packageName, classes) =>
          val sb = new StringBuffer()
          sb.append("/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */\n\n")
          sb.append(s"package ${packageName.split('.').reverse.drop(1).reverse}\n\n")
          sb.append("import io.surfkit.typebus._\n")
          sb.append("import io.surfkit.typebus.client._\n\n")
          sb.append(s"package object ${packageName.split('.').last}{\n\n")
          sb.append( classes.map(x => "  "+ x.caseClassRep).mkString("\n") )
          sb.append(s"\n\n  object Implicits extends AvroByteStreams{\n")
          sb.append( classes.map{ cc =>
            s"""
              |   implicit val ${cc.simpleName}Reader = new AvroByteStreamReader[${cc.simpleName}]
              |   implicit val ${cc.simpleName}Writer = new AvroByteStreamWriter[${cc.simpleName}]
            """.stripMargin
          }.mkString("") )
          sb.append(s"\n  }")
          // add the client mappings...
          sb.append("\n\n  /** Generated Actor Client */")

          sb.append(s"  class ${generator.serviceName}Client(implicit system: ActorSystem) extends Client(system){\n")
          val methodsInThisPackage = classes.flatMap(x => methodMap.get(x.fqn).map{ y => ServiceMethodGenerator(x.fqn, y) } )
          val fqlToCaseClass = classes.map(x => x.fqn -> x).toMap
          sb.append( methodsInThisPackage.map{ method =>
            val inType = fqlToCaseClass(method.in)
            val outType = fqlToCaseClass(method.out)
            s"   def ${inType.simpleName}(x: ${inType.simpleName}): Future[${outType.simpleName}] = wire[${inType.simpleName}, ${outType.simpleName}](x)"
          }.mkString("\n") )
          sb.append(s"\n  }")

          sb.append(s"\n}\n")
          (packageName, sb.toString)
      }.toList
    }

    def writeCodeToFiles(generator: ServiceGenerator) = {
      writeService(generator).foreach{
        case (packageName, sourceCode) =>
          val path = (List("src", "main", "scala") ::: packageName.split('.').toList).toArray
          val modelPath = Paths.get( path.mkString("/") )
          if(!Files.exists(modelPath))
            Files.createDirectories(modelPath)
          val filePath = Paths.get( path.mkString("/") + "/data2.scala" )
          println(s"modelPath: ${modelPath}")
          if(!Files.exists(filePath)){
            println(s"file path: ${filePath}")
            Files.write(filePath, sourceCode.getBytes)
          }

      }
    }
  }
}
