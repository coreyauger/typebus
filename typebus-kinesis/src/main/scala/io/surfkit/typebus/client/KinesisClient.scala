package io.surfkit.typebus.client

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.surfkit.typebus.{AvroByteStreams, ByteStreamReader, ByteStreamWriter}
import io.surfkit.typebus.actors.GatherActor
import io.surfkit.typebus.bus.Publisher
import io.surfkit.typebus.event.PublishedEvent
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

/***
  * Client - base class for generating RPC clients
  * @param system - akka actor system
  */
class KinesisClient(implicit system: ActorSystem) extends Client with Publisher with AvroByteStreams{
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

  val publishedEventWriter = new AvroByteStreamWriter[PublishedEvent]

  def publish(event: PublishedEvent): Unit =
    try {
      system.log.info(s"[KafkaClient] publish ${event.meta.eventType}")
      producer.send(
        new ProducerRecord[Array[Byte], Array[Byte]](
          event.meta.eventType,
          publishedEventWriter.write(event)
        )
      )
    }catch{
      case e:Exception =>
        system.log.error(e, "Error trying to publish event.")
    }


  /***
    * wire - function to create a Request per Actor and perform the needed type conversions.
    * @param x - This is the request type.  This is of type T.
    * @param timeout - configurable actor timeout with default of 4 seconds
    * @param w - ByteStreamWriter (defaults to avro)
    * @param r - ByteStreamReader (defaults to avro)
    * @tparam T - The IN type for the service call
    * @tparam U - The OUT type in the service called. Wrapped as Future[U]
    * @return - The Future[U] return from the service call.
    */
  def wire[T : ClassTag, U : ClassTag](x: T)(implicit timeout:Timeout = Timeout(4 seconds), w:ByteStreamWriter[T], r: ByteStreamReader[U]) :Future[U] = {
    val gather = system.actorOf(Props(new GatherActor[T, U](this, timeout, w, r)))
    (gather ? GatherActor.Request(x)).map(_.asInstanceOf[U])
  }
}


