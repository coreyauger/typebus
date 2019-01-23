package io.surfkit.typebus.gen

import akka.actor.{ActorSystem, Props}
import io.surfkit.typebus.AvroByteStreams
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import io.surfkit.typebus.bus.akka.AkkaBus
import io.surfkit.typebus.cluster.ZkClusterShardingSeedDiscovery

import scala.concurrent.duration._



class AkkaGenActor(args: Array[String]) extends Service[TypeBus]("code-gen") with AkkaBus[TypeBus] {
  implicit val system = context.system
  import context.dispatcher

  registerStream(genScalaServiceDescription("akka", List("src", "main", "scala")) _)
  startTypeBus

  val getServiceDescriptor = GetServiceDescriptor("")

  system.log.info("Waiting 7 sec to call getServiceDescriptor")
  context.system.scheduler.scheduleOnce(7 seconds) {
    publish(getServiceDescriptor)
  }
}

/***
  * App to generate source code for a service.
  * This is just a Typebus Service[TypeBus]
  */
object Main extends App with ZkClusterShardingSeedDiscovery{
  implicit val system = ActorSystem("squbs")  // TODO: get this from where? .. cfg?

  zkClusterSeed(system).join()
  system.actorOf(Props(new AkkaGenActor(args)))
}
