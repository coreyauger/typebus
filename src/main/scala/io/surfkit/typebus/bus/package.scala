package io.surfkit.typebus

import java.util.UUID

import akka.actor.ActorSystem
import io.surfkit.typebus.event.{EventMeta, PublishedEvent}
import io.surfkit.typebus.module.Service

import scala.reflect.ClassTag

package object bus {

  trait Publisher[UserBaseType]{
    def publish[T <: UserBaseType : ClassTag](obj: T)(implicit writer: ByteStreamWriter[T]): Unit =
      publish(PublishedEvent(
        meta = EventMeta(
          eventId = UUID.randomUUID().toString,
          eventType = obj.getClass.getCanonicalName,
          source = "",
          correlationId = Some(UUID.randomUUID().toString),
        ),
        payload = writer.write(obj)
      ))
    def publish(event: PublishedEvent): Unit
  }

  trait Bus[UserBaseType] extends Publisher[UserBaseType]{
    service: Service[UserBaseType] =>

    def startTypeBus(serviceName: String)(implicit system: ActorSystem): Unit
  }
}
