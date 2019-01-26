package io.surfkit.typebus.client

import java.io.{PrintWriter, StringWriter}
import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.surfkit.typebus.{AvroByteStreams, ByteStreamReader, ByteStreamWriter}
import io.surfkit.typebus.actors.GatherActor
import io.surfkit.typebus.bus.Publisher
import io.surfkit.typebus.event._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.time.Instant

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

/***
  * Client - base class for generating RPC clients
  * @param system - akka actor system
  */
class KafkaClient(serviceIdentifier: ServiceIdentifier)(implicit system: ActorSystem) extends Client with Publisher with AvroByteStreams{
  import akka.pattern.ask
  import collection.JavaConversions._
  import akka.util.Timeout
  import system.dispatcher

  val cfg = ConfigFactory.load
  val kafka = cfg.getString("bus.kafka")

  val producer = new KafkaProducer[Array[Byte], Array[Byte]](Map(
    "bootstrap.servers" -> kafka,
    "key.serializer" ->  "org.apache.kafka.common.serialization.ByteArraySerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer"
  ))

  val publishActor = system.actorOf(
    Props(new Actor {
      def receive = {
        case event: PublishedEvent =>
          try {
            system.log.info(s"[KafkaClient] publish ${event.meta.eventType}")
            producer.send(
              new ProducerRecord[Array[Byte], Array[Byte]](
                event.meta.eventType.fqn,
                publishedEventWriter.write(event)
              )
            )
          }catch{
            case t: Throwable =>
              produceErrorReport(serviceIdentifier)(t, event.meta)
          }
      }
    }))

  def publish(event: PublishedEvent)(implicit system: ActorSystem): Unit = publishActor ! event

  def busActor(implicit system: ActorSystem): ActorRef =
    publishActor

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
    val tType = scala.reflect.classTag[T].runtimeClass.getCanonicalName
    val uType = scala.reflect.classTag[U].runtimeClass.getCanonicalName
    val gather = system.actorOf(Props(new GatherActor[T, U](serviceIdentifier, this, timeout, w, r)))
    (gather ? GatherActor.Request(x)).map(_.asInstanceOf[U]).recoverWith{
      case t: Throwable =>
        produceErrorReport(serviceIdentifier)(t, EventMeta(
          eventId = UUID.randomUUID().toString,
          eventType = EventType.parse(x.getClass.getCanonicalName),
          source = "",
          directReply = None,
          correlationId = None
        ), s"FAILED RPC call ${tType} => Future[${uType}] failed with exception '${t.getMessage}'")
        Future.failed(t)
    }
  }
}


