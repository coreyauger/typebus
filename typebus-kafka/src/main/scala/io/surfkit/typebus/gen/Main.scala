package io.surfkit.typebus.gen

import akka.actor.ActorSystem
import avrohugger.Generator
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import avrohugger.format.Standard
import avrohugger.types.ScalaCaseObjectEnum
import io.surfkit.typebus.bus.kinesis.KafkaBus

import concurrent.Future

/***
  * App to generate source code for a service.
  * This is just a Typebus Service[TypeBus]
  */
object Main extends App with Service[TypeBus] with KafkaBus[TypeBus] {
  println("Typebus Generator with args: " + (args mkString ", "))

  implicit val system = ActorSystem("squbs")  // TODO: get this from where? .. cfg?
  implicit val serviceDescriptorReader = new AvroByteStreamReader[ServiceDescriptor]

  /***
    * genServiceDescription - this is in fact just a service function that responds to a broadcast for ServiceDefinitons
    * @param serviceDescriptor - The ServiceDescriptor received back from broadcast
    * @param meta - EventMeta The routing for this event.
    * @return - Unit this is a sink
    */
  def genServiceDescription(serviceDescriptor: ServiceDescriptor, meta: EventMeta): Future[Unit] = {
    println("getServiceDescription !!!!")
    println(serviceDescriptor)
    println(s"meta: ${meta}")

    //val serviceGenerator: ServiceGenerator =
    val generatedDescriptor = serviceDescriptor.serviceMethods.map{ serviceMethod =>
      val myScalaTypes = Some(Standard.defaultTypes.copy(enum = ScalaCaseObjectEnum))
      val generator = new Generator(Standard, avroScalaCustomTypes = myScalaTypes)
      println(s"Building case class for type: ${serviceMethod.in.fqn}")
      val inCaseClasses = generator.stringToStrings(serviceMethod.in.schema)
      println(s"Building case class for type: ${serviceMethod.out.fqn}")
      val outCaseClasses = generator.stringToStrings(serviceMethod.out.schema)

      def strCaseClassToGeneratedCaseClass(cc: String): Option[GeneratedCaseClass] = {
        // drop the comments
        cc.split("\n").drop(1).toList  match {
          case packageNameLine :: blank :: caseClass :: Nil =>
            val packageName = packageNameLine.replaceFirst("package ", "")
            val name = caseClass.replaceFirst("case class ", "").takeWhile(_ != '(')
            val fqn = Fqn(s"${packageName}.package.${name}")
            Some(GeneratedCaseClass(
              fqn = fqn,
              packageName = packageName,
              simpleName = name,
              caseClassRep = caseClass
            ))
          case _ =>
            println(s"WARNING could not parse case class: ${cc}")
            None
        }
      }
      val generatedInCaseClass = inCaseClasses.flatMap(strCaseClassToGeneratedCaseClass)
      val generatedOutCaseClass = outCaseClasses.flatMap(strCaseClassToGeneratedCaseClass)
      (ServiceMethodGenerator(Fqn(serviceMethod.in.fqn), Fqn(serviceMethod.out.fqn)), (generatedInCaseClass ::: generatedOutCaseClass).toSet[GeneratedCaseClass] )
    }.unzip

    val serviceGenerator = ServiceGenerator(serviceName = serviceDescriptor.service, methods = generatedDescriptor._1, caseClasses = generatedDescriptor._2.flatten.toSet[GeneratedCaseClass])

    println(s"generatedDescriptor: ${serviceGenerator}")
    ScalaCodeWriter.writeCodeToFiles(serviceGenerator)
    Future.successful(Unit)
  }

  registerStream(genServiceDescription _)
  startTypeBus("")

  val getServiceDescriptor = GetServiceDescriptor(args.last)
  implicit val getServiceDescriptorWriter = new AvroByteStreamWriter[GetServiceDescriptor]

  println(s"*** Calling getServiceDescriptor: ${getServiceDescriptor}")
  publish(getServiceDescriptor)
}
