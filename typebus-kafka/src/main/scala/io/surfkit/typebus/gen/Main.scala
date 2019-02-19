package io.surfkit.typebus.gen

import akka.actor.ActorSystem
import akka.cluster.seed.ZookeeperClusterSeed
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import io.surfkit.typebus.{AvroByteStreams, Typebus}
import io.surfkit.typebus.bus.kafka.KafkaBus
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import io.surfkit.typebus.cli._
import io.surfkit.typebus._
import scala.util.Try
import java.nio.file.{Files, Path, Paths}

/***
  * Cli
  */
object Main extends App {

  implicit val system = ActorSystem("squbs")  // TODO: get this from where? .. cfg?

  private class GeneratorService extends Service[TypeBus]("code-gen") with KafkaBus[TypeBus] with AvroByteStreams {

    // TODO: gen from bus..
    //registerStream(genScalaServiceDescription("Kafka", List("src", "main", "scala")) _)
    startTypeBus
    val getServiceDescriptor = GetServiceDescriptor(args.last)

    system.log.info(s"publish getServiceDescriptor: ${getServiceDescriptor} in 5 seconds")
    system.scheduler.scheduleOnce(5 seconds){
      system.log.info(s"publish ....")
      publish(getServiceDescriptor)
    }
  }

  // only want to activate and join cluster in certain cases
  //ZookeeperClusterSeed(system).join()

  CommandParser.runCli("Kafka")

  Thread.currentThread().join()
}
