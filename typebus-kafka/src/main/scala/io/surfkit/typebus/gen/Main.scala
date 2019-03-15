package io.surfkit.typebus.gen

import akka.actor.ActorSystem
import akka.cluster.seed.ZookeeperClusterSeed
import io.surfkit.typebus.module.Service
import io.surfkit.typebus.event.{GetServiceDescriptor, ServiceIdentifier}
import io.surfkit.typebus.cli._
import io.surfkit.typebus.bus.TypebusApplication
import io.surfkit.typebus.bus.kafka.{TypebusKafkaConsumer, TypebusKafkaProducer}
import scala.concurrent.duration._

/***
  * Cli
  */
object Main extends App {

  implicit val system = ActorSystem("squbs")  // TODO: get this from where? .. cfg?
  lazy val serviceIdentity = ServiceIdentifier("gen-code-service")

  // only want to activate and join cluster in certain cases
  //ZookeeperClusterSeed(system).join()
  lazy val producer = new TypebusKafkaProducer(serviceIdentity, system)
  lazy val service = new Service(serviceIdentity, producer){
    import system.dispatcher

    //registerStream(genScalaServiceDescription("Kafka", List("src", "main", "scala")) _)
    val getServiceDescriptor = GetServiceDescriptor(args.last)

    system.log.info(s"publish getServiceDescriptor: ${getServiceDescriptor} in 5 seconds")
    system.scheduler.scheduleOnce(5 seconds){
      system.log.info(s"publish ....")
      producer.publish(getServiceDescriptor)
    }

  }
  lazy val consumer = new TypebusKafkaConsumer(service, producer, system)

  println("\n\n***********\n\n")
  TypebusApplication
  (
    system,
    serviceIdentity,
    producer,
    service,
    consumer
  )


  CommandParser.runCli("Kafka")

  Thread.currentThread().join()
}
