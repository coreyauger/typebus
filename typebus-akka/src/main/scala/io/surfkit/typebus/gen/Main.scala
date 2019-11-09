package io.surfkit.typebus.gen

import akka.actor.ActorSystem
import io.surfkit.typebus.module.Service
import io.surfkit.typebus.event.ServiceIdentifier
import io.surfkit.typebus.cli._
import io.surfkit.typebus.bus.akka.{AkkaBusConsumer, AkkaBusProducer}

import scala.concurrent.duration._

/***
  * Cli
  */
object Main extends App {

  implicit val system = ActorSystem("codegen")
  lazy val serviceIdentity = ServiceIdentifier("gen-code-service")

  // only want to activate and join cluster in certain cases
  //ZookeeperClusterSeed(system).join()
  lazy val producer = new AkkaBusProducer(serviceIdentity, system)
  lazy val service = new Service(serviceIdentity, producer){
  }
  lazy val consumer = new AkkaBusConsumer(service, producer, system)

  println("\n\n***********\n\n")
  CommandParser.runCli

  Thread.currentThread().join()
}
