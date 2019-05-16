package io.surfkit.typebus

import java.io.{PrintWriter, StringWriter}
import java.time.Instant
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.ConfigFactory
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Try

package object bus {

  trait RetryBackoff
  object RetryBackoff{
    case object None extends RetryBackoff
    case object Linear extends RetryBackoff
    case object Exponential extends RetryBackoff
  }

  case class RetryPolicy(numRetry: Int, delay: FiniteDuration, backoff: RetryBackoff)
  object RetryPolicy{
    val Fail = RetryPolicy(0, 0 seconds, RetryBackoff.None)
  }


  trait StreamBuilder[T, U]{
    var partitionKey: Option[U => String] = None
    var retry: Option[PartialFunction[Throwable, RetryPolicy]] = None

    def withPartitionKey(f: U => String): StreamBuilder[T, U] = {
      partitionKey = Some(f)
      this
    }
    def withRetryPolicy(pf: PartialFunction[Throwable, RetryPolicy]): StreamBuilder[T, U] = {
      retry = Some(pf)
      this
    }
    def untyped(x: Any): Option[String] =
      Try( partitionKey.map(_.apply(x.asInstanceOf[U]) ) ).toOption.flatten
  }

  /***
    *  Publisher
    */
  abstract class Publisher extends AvroByteStreams{
    def publish[T : ClassTag](obj: T)(implicit writer: ByteStreamWriter[T], system: ActorSystem): Unit =
      publish(PublishedEvent(
        meta = EventMeta(
          eventId = UUID.randomUUID().toString,
          eventType = EventType.parse(obj.getClass.getCanonicalName),
          source = "",
          correlationId = Some(UUID.randomUUID().toString),
        ),
        payload = writer.write(obj)
      ))

    def serviceIdentifier: ServiceIdentifier
    def publish(event: PublishedEvent)(implicit system: ActorSystem): Unit
    def busActor(implicit system: ActorSystem): ActorRef

    val trace = ConfigFactory.load.getBoolean("bus.trace")

    def traceEvent( event: Trace, meta: EventMeta)(implicit system: ActorSystem): Unit = {
      if(  // CA - well this is lame :(
        (trace || meta.trace) &&
          !meta.eventType.fqn.endsWith(InEventTrace.getClass.getSimpleName.replaceAllLiterally("$","")) &&
          !meta.eventType.fqn.endsWith(OutEventTrace.getClass.getSimpleName.replaceAllLiterally("$","")) &&
          !meta.eventType.fqn.endsWith(ExceptionTrace.getClass.getSimpleName.replaceAllLiterally("$",""))
      ){
        busActor ! PublishedEvent(
          meta = meta.copy(
            eventId = UUID.randomUUID().toString,
            eventType =  EventType.parse(event.getClass.getCanonicalName),
            trace = false,
            occurredAt = Instant.now
          ),
          payload = event match{
            case x: OutEventTrace => OutEventTraceWriter.write(x)
            case x: InEventTrace => InEventTraceWriter.write(x)
            case x: ExceptionTrace => ExceptionTraceWriter.write(x)
          }
        )
      }
    }


    def produceErrorReport(t: Throwable, meta: EventMeta, msg: String = "typebus caught exception")(implicit system: ActorSystem) = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      val ex = ServiceException(
        message = msg + s"\n${t.getMessage}",
        throwableType = t.getClass.getCanonicalName,
        stackTrace = sw.toString.split("\n").toSeq
      )
      // Close any Rpc Client Futures.. (they return Either[ServiceException, U])
      // RPC clients publish to the "Serivice Name" subscription, where that service then can route message back to RPC client.
      meta.directReply.filterNot(_.service.name == serviceIdentifier.name).foreach { rpc =>
        publish(
          PublishedEvent(
            meta = meta.copy(eventType = EventType.parse(rpc.service.name)),
            payload = ServiceExceptionWriter.write(ex)
          )
        )
      }
      traceEvent(
        ExceptionTrace(serviceIdentifier, PublishedEvent(
          meta = EventMeta(
            eventId = UUID.randomUUID().toString,
            source = "",
            eventType = EventType.parse(ex.getClass.getCanonicalName),
            correlationId = None,
            trace = true
          ),
          payload = ServiceExceptionWriter.write(ex)
        )), meta)
      system.log.error(msg,t)
      ex
    }
  }


  /***
    * Consumer
    */
  trait Consumer extends AvroByteStreams{
    def service: Service
    //def startTypeBus(implicit system: ActorSystem): Unit

    def consume(publish: PublishedEvent)(implicit system: ActorSystem) = {
      system.log.info(s"typebus - consume event: ${publish.meta.eventType}")
      if(publish.meta.directReply.map(_.service.name == service.serviceIdentifier.name).getOrElse[Boolean](false))    // handel rpc first since we won't have a key in "listOfServiceImplicitsReaders"
        service.handleRpcReply(publish)
      else {
        val reader = service.listOfServiceImplicitsReaders.get(publish.meta.eventType).getOrElse(service.listOfImplicitsReaders(publish.meta.eventType))
        val payload = reader.read(publish.payload)
        if (service.handleEventWithMetaUnit.isDefinedAt((payload, publish.meta)))
          service.handleEventWithMetaUnit((payload, publish.meta))
        else if (service.handleEventWithMeta.isDefinedAt((payload, publish.meta)))
          service.handleEventWithMeta((payload, publish.meta))
        else if (service.handleServiceEventWithMeta.isDefinedAt((payload, publish.meta)))
          service.handleServiceEventWithMeta((payload, publish.meta))
        else
          throw new RuntimeException(s"No method defined for typebue type: ${publish.meta.eventType}.  Something is very wrong !!")
      }
    }
  }


  case class TypebusApplication(
               system: ActorSystem,
               serviceId: ServiceIdentifier,
               publisher: Publisher,
               service: Service,
               consumer: Consumer
             )
}
