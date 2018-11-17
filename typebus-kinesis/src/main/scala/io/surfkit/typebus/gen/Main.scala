package io.surfkit.typebus.gen

import akka.actor.{ActorSystem, Props}
import avrohugger.Generator
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import avrohugger.format.Standard
import avrohugger.types.ScalaCaseObjectEnum
import io.surfkit.typebus.bus.kinesis.KinesisBus
import io.surfkit.typebus.cluster.ZkCluserShadingSeedDiscovery

import scala.concurrent.duration._
import concurrent.Future


class KinesisGenActor extends Service[TypeBus] with KinesisBus[TypeBus] {
  implicit val serviceDescriptorReader = new AvroByteStreamReader[ServiceDescriptor]
  implicit val system = context.system
  import context.dispatcher
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

  val getServiceDescriptor = GetServiceDescriptor("twitter")
  implicit val getServiceDescriptorWriter = new AvroByteStreamWriter[GetServiceDescriptor]

  println("Waiting 2 sec to call getServiceDescriptor")
  context.system.scheduler.scheduleOnce(15 seconds) {
    println(s"*** Calling getServiceDescriptor: ${getServiceDescriptor}")
    publish(getServiceDescriptor)
  }
}

/***
  * App to generate source code for a service.
  * This is just a Typebus Service[TypeBus]
  */
object Main extends App with ZkCluserShadingSeedDiscovery{
  println("Typebus Generator with args: " + (args mkString ", "))

  /*class ServiceThread(squbs: String, args: Array[String]) extends Thread {
    override def run() {
      println(s"squbs: ${squbs}")
      println(s"Class: ${Class.forName(squbs)}")
      val methods = Class.forName(squbs).getMethods
      val main = methods.filter(_.getName == "main").head
      main.invoke(null, args.drop(1))
    }
  }*/

  implicit val system = ActorSystem("squbs")  // TODO: get this from where? .. cfg?
  zkCluserSeed(system).join()
  system.actorOf(Props(new KinesisGenActor))

  //val squbs = "org.squbs.unicomplex.Bootstrap"  // TODO: this is arg(0)
  //val thread = new ServiceThread(squbs, args)
  //thread.setDaemon(false)
  //thread.start()
  //println("THREAD JOIN ==========================================================================================")
  //thread.join()
}
