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
import io.surfkit.typebus._

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


  @inline def defined(line: String) = {
    line != null && line.nonEmpty
  }
  println("reading console input..")
  Iterator.continually(scala.io.StdIn.readLine).takeWhile(defined(_)).foreach { line =>
    println("cli read: " + line)
    CommandParser.parse(line.split(' ')).foreach{
      case cmd if cmd.command == Cmd.CodeGen =>
        val genCmd = cmd.gen
        if(genCmd.service != ""){
          ZookeeperClusterSeed(system).join()
          new GeneratorService
        }else if(genCmd.push){
          // get the project to push to
          println("push code gen...")
          val generated = gen.selfCodeGen
          genCmd.out.foreach { outFile =>
            gen.ScalaCodeWriter.writeCodeToFiles("Kafka", generated, outFile.getAbsolutePath.split('/').toList )
          }
        }else if(!genCmd.target.isEmpty){
          val target = genCmd.target.get
          val generated = gen.codeGen(target.toPath)
          genCmd.out.foreach { outFile =>
            gen.ScalaCodeWriter.writeCodeToFiles("Kafka", generated, outFile.getAbsolutePath.split('/').toList )
          }
        }


      case cmd =>
        println(s"Unknown or unsupported command. ${cmd}")
    }
  }

  Thread.currentThread().join()
}
