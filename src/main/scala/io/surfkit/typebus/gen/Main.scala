package io.surfkit.typebus.gen

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import avrohugger.Generator
import com.typesafe.config.ConfigFactory
import io.surfkit.typebus.event.{EventMeta, ServiceDescriptor, TypeBus}
import io.surfkit.typebus.module.Service
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import avrohugger.format.Standard
import avrohugger.types.ScalaCaseObjectEnum

import concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import concurrent.duration._


object Main extends App with Service[TypeBus] {
  Console.println("Typebus Generator with args: " + (args mkString ", "))

  class ServiceThread(squbs: String, args: Array[String]) extends Thread {

    override def run() {
      println(s"squbs: ${squbs}")
      println(s"Class: ${Class.forName(squbs)}")
      val methods = Class.forName(squbs).getMethods
      val main = methods.filter(_.getName == "main").head
      main.invoke(null, args.drop(1))
    }
  }

  // TODO: need to abstract away the bus layer more then this..
  val cfg = ConfigFactory.load

  val kafka = cfg.getString("bus.kafka")
  implicit val system = ActorSystem("squbs")  // TODO: get this from where?

  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers(kafka)
    .withGroupId("tally")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  implicit val serviceDescriptorReader = new AvroByteStreamReader[ServiceDescriptor]

  def getServiceDescription(serviceDescriptor: ServiceDescriptor, meta: EventMeta): Future[Unit] = {
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

    val serviceGenerator = ServiceGenerator(serviceName = "SomeName", methods = generatedDescriptor._1, caseClasses = generatedDescriptor._2.flatten.toSet[GeneratedCaseClass])

    println(s"generatedDescriptor: ${serviceGenerator}")
    ScalaCodeWriter.writeCodeToFiles(serviceGenerator)
    Future.successful(Unit)
  }


  registerStream(getServiceDescription _)
  startService("",consumerSettings, akka.actor.ActorRef.noSender)

  val squbs = "org.squbs.unicomplex.Bootstrap"  // TODO: this is arg(0)
  val thread = new ServiceThread(squbs, args)
  thread.setDaemon(false)
  thread.start()
  println("THREAD JOIN ==========================================================================================")
  thread.join()
}
