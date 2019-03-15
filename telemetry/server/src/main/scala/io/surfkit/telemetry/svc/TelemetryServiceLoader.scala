package io.surfkit.telemetry.svc

import akka.actor._
import io.surfkit.typebus.bus.TypebusApplication
import io.surfkit.typebus.bus.kafka.{TypebusKafkaConsumer, TypebusKafkaProducer}
import io.surfkit.typebus.event.ServiceIdentifier

class TelemetryServiceLoader extends Actor with ActorLogging{
  implicit val system = context.system

  lazy val serviceIdentity = ServiceIdentifier("telemetry")

  // only want to activate and join cluster in certain cases
  //ZookeeperClusterSeed(system).join()
  lazy val producer = new TypebusKafkaProducer(serviceIdentity, system)
  lazy val service = new TelemetryService(serviceIdentity, producer, system)
  println("\nCREATING CONSUMER !!!")
  lazy val consumer = new TypebusKafkaConsumer(service, producer, system)

  println("\n\n")
  println("*************************")
  TypebusApplication
  (
    system,
    serviceIdentity,
    producer,
    service,
    consumer
  )

  override def receive = {
    case _ =>
  }
}

