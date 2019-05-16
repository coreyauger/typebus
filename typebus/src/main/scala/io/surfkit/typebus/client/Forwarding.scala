package io.surfkit.typebus.client

import java.util.UUID

import akka.actor.ActorSystem
import io.surfkit.typebus.ByteStreamWriter
import io.surfkit.typebus.bus.Publisher
import io.surfkit.typebus.event._
import scala.reflect.ClassTag

trait Forwarding {

  def forward[T : ClassTag](publisher: Publisher, x: T, caller: RpcClient, correlationId: Option[String] = None)(implicit w:ByteStreamWriter[T], system: ActorSystem) : Unit= {
    val tType = scala.reflect.classTag[T].runtimeClass.getCanonicalName
    val meta =
      EventMeta(
        eventId = UUID.randomUUID().toString,
        eventType = EventType.parse(tType),
        directReply = Some(caller),
        correlationId = correlationId
      )
    publisher.publish(PublishedEvent(
      meta = meta,
      payload = w.write(x)
    ))
  }
}
