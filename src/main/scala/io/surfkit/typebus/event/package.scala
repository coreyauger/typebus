package io.surfkit.typebus

package object event {

  sealed trait TypeBus{}

  trait EventType{
    def fqn: String
  }

  case class InType(fqn: String) extends EventType
  case class OutType(fqn: String) extends EventType

  case class ServiceMethod(in: InType, out: OutType) extends TypeBus


  case class ServiceDescriptor(
                              name: String,
                              schemaRepoUrl: String,
                              serviceMethods: Seq[ServiceMethod]
                              ) extends TypeBus

  case class EventMeta(eventId: String,
                       eventType: String,
                       source: String,
                       correlationId: Option[String],
                       directReply: Option[String] = None,
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






