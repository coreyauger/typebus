package io.surfkit.typebus

import java.io.{PrintWriter, StringWriter}
import java.time.Instant
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.ConfigFactory
import io.surfkit.typebus.event._
import io.surfkit.typebus.gen._
import io.surfkit.typebus.module.Service

import scala.concurrent.Future
import scala.reflect.ClassTag

package object bus {


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
        message = msg,
        stackTrace = sw.toString.split("\n").toSeq
      )
      traceEvent(
        ExceptionTrace(serviceIdentifier.service, serviceIdentifier.serviceId, PublishedEvent(
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
    }
  }


  /***
    * Consumer
    */
  trait Consumer extends AvroByteStreams{
    def service: Service
    //def startTypeBus(implicit system: ActorSystem): Unit

    def consume(publish: PublishedEvent)(implicit system: ActorSystem) = {
      val reader = service.listOfServiceImplicitsReaders.get(publish.meta.eventType).getOrElse(service.listOfImplicitsReaders(publish.meta.eventType))
      val payload = reader.read(publish.payload)
      if(service.handleEventWithMetaUnit.isDefinedAt( (payload, publish.meta) ) )
        service.handleEventWithMetaUnit( (payload, publish.meta) )
      else if(service.handleEventWithMeta.isDefinedAt( (payload, publish.meta) ) )
        service.handleEventWithMeta( (payload, publish.meta)  )
      else if(service.handleServiceEventWithMeta.isDefinedAt( (payload, publish.meta) ) )
        service.handleServiceEventWithMeta( (payload, publish.meta)  )
      else if(publish.meta.directReply.map(_.service.service == service.serviceName).getOrElse[Boolean](false))
        service.handleRpcReply(publish)
      else
        throw new RuntimeException(s"No method defined for typebue type: ${publish.meta.eventType}.  Something is very wrong !!")
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
