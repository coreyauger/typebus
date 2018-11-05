package io.surfkit.typebus

package object event {

  sealed trait TypeBus{}

  case class EventMeta(eventId: String,
                       eventType: String,
                       source: String,
                       correlationId: Option[String],
                       userId: Option[String] = None,
                       socketId: Option[String] = None,
                       responseTo: Option[String] = None,
                       extra: Map[String, String] = Map.empty
                       /*occurredAt: DateTime*/) extends TypeBus


  final case class SocketEvent(
                             meta: EventMeta,
                             payload: Array[Byte]
                           ) extends TypeBus

  final case class PublishedEvent(
                             meta: EventMeta,
                             payload: Array[Byte]
                              ) extends TypeBus
}






