package io.surfkit.typebus.client

import java.io.{PrintWriter, StringWriter}
import java.util.UUID

import akka.actor.{ActorSystem, Props}
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
  val trace = cfg.getBoolean("bus.trace")
  val kafka = cfg.getString("bus.kafka")

  val producer = new KafkaProducer[Array[Byte], Array[Byte]](Map(
    "bootstrap.servers" -> kafka,
    "key.serializer" ->  "org.apache.kafka.common.serialization.ByteArraySerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer"
  ))

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

  def traceEvent( f:(ServiceIdentifier) => Trace, meta: EventMeta): Unit = {
    if(  // CA - well this is lame :(
      (trace || meta.trace) &&
        !meta.eventType.endsWith(InEventTrace.getClass.getSimpleName.replaceAllLiterally("$","")) &&
        !meta.eventType.endsWith(OutEventTrace.getClass.getSimpleName.replaceAllLiterally("$","")) &&
        !meta.eventType.endsWith(ExceptionTrace.getClass.getSimpleName.replaceAllLiterally("$",""))
    ){
      val event = f(serviceIdentifier)
      publish(PublishedEvent(
        meta = meta.copy(
          eventId = UUID.randomUUID().toString,
          eventType = event.getClass.getCanonicalName,
          correlationId = None,
          trace = false,
          occurredAt = Instant.now
        ),
        payload = event match{
          case x: OutEventTrace => OutEventTraceWriter.write(x)
          case x: InEventTrace => InEventTraceWriter.write(x)
          case x: ExceptionTrace => ExceptionTraceWriter.write(x)
        }
      ))
    }
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
    (gather ? GatherActor.Request(x)).map(_.asInstanceOf[U]).recoverWith{
      case t: Throwable =>
        val sw = new StringWriter
        t.printStackTrace(new PrintWriter(sw))
        val ex = ServiceException(
          message = t.getMessage,
          stackTrace = sw.toString.split("\n").toSeq
        )
        traceEvent( { serviceIdentifier: ServiceIdentifier =>
          ExceptionTrace(serviceIdentifier.service, serviceIdentifier.serviceId, PublishedEvent(
            meta = EventMeta(
              eventId = UUID.randomUUID().toString,
              source = "",
              eventType = ex.getClass.getCanonicalName,
              correlationId = None,
              trace = true
            ),
            payload = ServiceExceptionWriter.write(ex)))
        }, EventMeta(
          eventId = UUID.randomUUID().toString,
          eventType = x.getClass.getCanonicalName,
          source = "",
          directReply = None,
          correlationId = None
        ))
        Future.failed(t)
    }
  }
}


