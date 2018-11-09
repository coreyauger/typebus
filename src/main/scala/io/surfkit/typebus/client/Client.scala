package io.surfkit.typebus.client

import akka.actor.{ActorSystem, PoisonPill, Props}
import com.typesafe.config.ConfigFactory
import io.surfkit.typebus.{ByteStreamReader, ByteStreamWriter}
import io.surfkit.typebus.actors.GatherActor
import io.surfkit.typebus.event.PublishedEvent
import org.apache.kafka.clients.producer.KafkaProducer

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
  * Created by suroot on 21/12/16.
  */

class Client(implicit system: ActorSystem){
  import akka.pattern.ask
  import collection.JavaConversions._
  import akka.util.Timeout
  import system.dispatcher

  val kafka = ConfigFactory.load.getString("bus.kafka")

  val producer = new KafkaProducer[Array[Byte], Array[Byte]](Map(
    "bootstrap.servers" -> kafka,
    "key.serializer" ->  "org.apache.kafka.common.serialization.ByteArraySerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer"
  ))

  def wire[T : ClassTag, U : ClassTag](x: T)(implicit timeout:Timeout = Timeout(4 seconds), w:ByteStreamWriter[T], r: ByteStreamReader[U]) :Future[U] = {
    val gather = system.actorOf(Props(new GatherActor[T, U](producer, timeout, w, r)))
    (gather ? GatherActor.Request(x)).map(_.asInstanceOf[U])
  }
}


