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
  trait Publisher extends AvroByteStreams{
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

    def publish(event: PublishedEvent)(implicit system: ActorSystem): Unit
    def busActor(implicit system: ActorSystem): ActorRef

    val trace = ConfigFactory.load.getBoolean("bus.trace")

    def traceEvent(serviceIdentifier: ServiceIdentifier)( f: (ServiceIdentifier) => Trace, meta: EventMeta)(implicit system: ActorSystem): Unit = {
      if(  // CA - well this is lame :(
        (trace || meta.trace) &&
          !meta.eventType.fqn.endsWith(InEventTrace.getClass.getSimpleName.replaceAllLiterally("$","")) &&
          !meta.eventType.fqn.endsWith(OutEventTrace.getClass.getSimpleName.replaceAllLiterally("$","")) &&
          !meta.eventType.fqn.endsWith(ExceptionTrace.getClass.getSimpleName.replaceAllLiterally("$",""))
      ){
        val event = f( serviceIdentifier)
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


    def produceErrorReport(serviceIdentifier: ServiceIdentifier)(t: Throwable, meta: EventMeta, msg: String = "typebus caught exception")(implicit system: ActorSystem) = {
      val sw = new StringWriter
      t.printStackTrace(new PrintWriter(sw))
      val ex = ServiceException(
        message = msg,
        stackTrace = sw.toString.split("\n").toSeq
      )
      traceEvent(serviceIdentifier)( { s: ServiceIdentifier =>
        ExceptionTrace(s.service, s.serviceId, PublishedEvent(
          meta = EventMeta(
            eventId = UUID.randomUUID().toString,
            source = "",
            eventType = EventType.parse(ex.getClass.getCanonicalName),
            correlationId = None,
            trace = true
          ),
          payload = ServiceExceptionWriter.write(ex)
        ))
      }, meta)
      system.log.error(msg,t)
    }
  }


  /***
    * Bus
    * @tparam UserBaseType
    */
  trait Bus[UserBaseType] extends Publisher{
    service: Service[UserBaseType] =>

    def startTypeBus(implicit system: ActorSystem): Unit

    def consume(publish: PublishedEvent) = {
      val reader = listOfServiceImplicitsReaders.get(publish.meta.eventType).getOrElse(listOfImplicitsReaders(publish.meta.eventType))
      val payload = reader.read(publish.payload)
      if(handleEventWithMetaUnit.isDefinedAt( (payload, publish.meta) ) )
        handleEventWithMetaUnit( (payload, publish.meta) )
      else if(handleEventWithMeta.isDefinedAt( (payload, publish.meta) ) )
        handleEventWithMeta( (payload, publish.meta)  )
      else if(handleServiceEventWithMeta.isDefinedAt( (payload, publish.meta) ) )
        handleServiceEventWithMeta( (payload, publish.meta)  )
      else
        handleEvent(payload)
    }

    def traceEvent( f: (ServiceIdentifier) => Trace, meta: EventMeta)(implicit system: ActorSystem) = super.traceEvent(ServiceIdentifier(service.serviceName, service.serviceId)) _
    def produceErrorReport(t: Throwable, meta: EventMeta, msg: String)(implicit system: ActorSystem) = super.produceErrorReport(ServiceIdentifier(service.serviceName, service.serviceId)) _
  }
}
