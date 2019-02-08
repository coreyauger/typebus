package io.surfkit.typebus.gen

import akka.actor.ActorSystem
import akka.cluster.seed.ZookeeperClusterSeed
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import io.surfkit.typebus.{AvroByteStreams, Typebus}
import io.surfkit.typebus.bus.kafka.KafkaBus
import io.surfkit.typebus.cli.CommandParser.Cmd

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import io.surfkit.typebus.cli._

import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

/***
  * Cli
  */
object Main extends App {

  implicit val system = ActorSystem("squbs")  // TODO: get this from where? .. cfg?

  private class GeneratorService extends Service[TypeBus]("code-gen") with KafkaBus[TypeBus] with AvroByteStreams {
    registerStream(genScalaServiceDescription("Kafka", List("src", "main", "scala")) _)
    startTypeBus
    val getServiceDescriptor = GetServiceDescriptor(args.last)

    system.log.info(s"publish getServiceDescriptor: ${getServiceDescriptor} in 5 seconds")
    system.scheduler.scheduleOnce(5 seconds){
      system.log.info(s"publish ....")
      publish(getServiceDescriptor)
    }
  }


  @inline def defined(line: String) = {
    line != null && line.nonEmpty
  }
  println("reading console input..")
  Iterator.continually(scala.io.StdIn.readLine).takeWhile(defined(_)).foreach { line =>
    println("cli read: " + line)
    CommandParser.parse(line.split(' ')).foreach{
      case cmd if cmd.command == Cmd.CodeGen =>
        val gen = cmd.gen
        if(gen.service != ""){
          ZookeeperClusterSeed(system).join()
          new GeneratorService
        }else if(gen.push){
          // get the project to push to
          println("push code gen...")
          Typebus.selfCodeGen
        }
        // TODO..


      case cmd =>
        println(s"Unknown or unsupported command. ${cmd}")
    }
  }

  Thread.currentThread().join()
}
