package io.surfkit.typebus.client

import java.io.{PrintWriter, StringWriter}
import java.nio.ByteBuffer
import java.util.UUID

import com.typesafe.config.ConfigFactory
import akka.actor.{ActorRef, ActorSystem, Props, Actor}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import io.surfkit.typebus.{AvroByteStreams, ByteStreamReader, ByteStreamWriter}
import io.surfkit.typebus.actors.GatherActor
import io.surfkit.typebus.bus.Publisher
import io.surfkit.typebus.event._
import org.joda.time.DateTime

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

/***
  * Client - base class for generating RPC clients
  * @param system - akka actor system
  */
class AkkaClient(service: ServiceIdentifier)(implicit system: ActorSystem) extends Client with Publisher with AvroByteStreams {
  import akka.pattern.ask
  import akka.util.Timeout
  import system.dispatcher

  val cfg = ConfigFactory.load
  val trace = cfg.getBoolean("bus.trace")
  val tyebusMap = scala.collection.mutable.Map.empty[String, ActorRef]

  val mediator = DistributedPubSub(system).mediator

  val publishActor = system.actorOf(
    Props(new Actor {
      import akka.cluster.pubsub.DistributedPubSubMediator.Publish
      def receive = {
        case event: PublishedEvent => mediator ! Publish(event.meta.eventType.fqn, event, sendOneMessageToEachGroup=true )
      }
    }))


  def traceEvent( f:(ServiceIdentifier) => Trace, meta: EventMeta): Unit = {
    if(  // CA - well this is lame :(
      (trace || meta.trace) &&
        !meta.eventType.fqn.endsWith(InEventTrace.getClass.getSimpleName.replaceAllLiterally("$","")) &&
        !meta.eventType.fqn.endsWith(OutEventTrace.getClass.getSimpleName.replaceAllLiterally("$","")) &&
        !meta.eventType.fqn.endsWith(ExceptionTrace.getClass.getSimpleName.replaceAllLiterally("$",""))
    ){
      val event = f(service)
      publishActor ! PublishedEvent(
        meta = meta.copy(
          eventId = UUID.randomUUID().toString,
          eventType = event.getClass.getCanonicalName,
          correlationId = None,
          trace = false,
          occurredAt = DateTime.now
        ),
        payload = event match{
          case x: OutEventTrace => OutEventTraceWriter.write(x)
          case x: InEventTrace => InEventTraceWriter.write(x)
          case x: ExceptionTrace => ExceptionTraceWriter.write(x)
        }
      )
    }
  }

  def publish(event: PublishedEvent): Unit =
    try {
      system.log.info(s"[KinesisClient] publish ${event.meta.eventType}")
      publishActor ! event
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
    val tType = scala.reflect.classTag[T].runtimeClass.getCanonicalName
    val uType = scala.reflect.classTag[U].runtimeClass.getCanonicalName
    val gather = system.actorOf(Props(new GatherActor[T, U](this, timeout, w, r)))
    (gather ? GatherActor.Request(x)).map(_.asInstanceOf[U]).recoverWith{
      case t: Throwable =>
        val sw = new StringWriter
        t.printStackTrace(new PrintWriter(sw))
        val ex = ServiceException(
          message = s"FAILED RPC call ${tType} => Future[${uType}] failed with exception '${t.getMessage}'",
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


