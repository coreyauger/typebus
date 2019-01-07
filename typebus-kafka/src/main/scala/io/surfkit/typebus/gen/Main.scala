package io.surfkit.typebus.gen

import akka.actor.ActorSystem
import akka.cluster.seed.ZookeeperClusterSeed
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import io.surfkit.typebus.AvroByteStreams
import io.surfkit.typebus.bus.kafka.KafkaBus
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/***
  * App to generate source code for a service.
  * This is just a Typebus Service[TypeBus]
  */
object Main extends App {

  implicit val system = ActorSystem("squbs")  // TODO: get this from where? .. cfg?

  ZookeeperClusterSeed(system).join()

  class GeneratorService extends Service[TypeBus]("code-gen") with KafkaBus[TypeBus] with AvroByteStreams {

    registerStream(genScalaServiceDescription("Kafka", List("src", "main", "scala")) _)
    startTypeBus

    val getServiceDescriptor = GetServiceDescriptor(args.last)

    system.log.info(s"publish getServiceDescriptor: ${getServiceDescriptor} in 5 seconds")
    system.scheduler.scheduleOnce(5 seconds){
      system.log.info(s"publish ....")
      publish(getServiceDescriptor)
    }

  }

  val gen = new GeneratorService

  Thread.currentThread().join()
}
