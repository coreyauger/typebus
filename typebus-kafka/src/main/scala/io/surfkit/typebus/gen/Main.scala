package io.surfkit.typebus.gen

import akka.actor.ActorSystem
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import io.surfkit.typebus.AvroByteStreams
import io.surfkit.typebus.bus.kafka.KafkaBus


/***
  * App to generate source code for a service.
  * This is just a Typebus Service[TypeBus]
  */
object Main extends App {


  implicit val system = ActorSystem("squbs")  // TODO: get this from where? .. cfg?

  object Service extends Service[TypeBus]("") with KafkaBus[TypeBus] with AvroByteStreams {
    registerStream(genScalaServiceDescription("Kafka", List("src", "main", "scala")) _)
    startTypeBus

    val getServiceDescriptor = GetServiceDescriptor(args.last)


    system.log.debug(s"publish getServiceDescriptor: ${getServiceDescriptor}")
    publish(getServiceDescriptor)
  }
}
