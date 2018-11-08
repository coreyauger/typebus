package io.surfkit.typebus.gen

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import com.typesafe.config.ConfigFactory
import io.surfkit.typebus.event.{EventMeta, ServiceDescriptor, TypeBus}
import io.surfkit.typebus.module.Service
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import concurrent.duration._

object Main extends App with Service[TypeBus] {
  Console.println("Typebus Generator with args: " + (args mkString ", "))

  // TODO: setup the hooks to listen for the service broadcast events.
  val cfg = ConfigFactory.load

  val kafka = cfg.getString("bus.kafka")
  implicit val system = ActorSystem("squbs")

  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers(kafka)
    .withGroupId("tally")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  implicit val serviceDescriptorReader = new AvroByteStreamReader[ServiceDescriptor]

  def getServiceDescription(x: ServiceDescriptor, meta: EventMeta): Future[Unit] = {
    println("WE GOT A PIZZA HERE sports fans !!!!")
    println(x)
    println(s"meta: ${meta}")
    Future.successful(Unit)
  }

  println("Running !!!!")

  registerStream(getServiceDescription _)


  //startService("",consumerSettings, akka.actor.ActorRef.noSender)

  val squbs = "org.squbs.unicomplex.Bootstrap"  // TODO: this is arg(0)
  /// https://stackoverflow.com/questions/1469958/scala-how-do-i-dynamically-instantiate-an-object-and-invoke-a-method-using-refl
  println(s"Class: ${Class.forName(squbs)}")
  val methods = Class.forName(squbs).getMethods
  println(s"con: ${methods}")
  println(s"con: ${methods.length}")
  println(s"con: ${methods.mkString(",")}")
  val main = methods.filter(_.getName == "main").head
  println(s"main: ${main}")

  val f = Future {
    //entry.main(args.drop(1))
    main.invoke(null, args.drop(1))
  }

  Await.result(f, 10 seconds)
  Thread.currentThread().join()
}
