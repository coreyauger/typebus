package io.surfkit.typebus

package object event {

  /***
    * TypeBus - base type
    */
  sealed trait TypeBus{}

  /***
    *  EventType - wrap the Fully Qualified Name of a type
    */
  trait EventType{
    /***
      * fqn - Fully Qualified Name
      * @return - return the fully qualified name of a type.
      */
    def fqn: String
  }

  /***
    * InType - This is the type passed IN to a service method
    * @param fqn - Fully Qualified Name of Type
    * @param schema - The Avro Schema
    */
  case class InType(fqn: String, schema: String) extends EventType

  /***
    * OutType - This is the type passed OUT of a service function
    * @param fqn - Fully Qualified Name of Type
    * @param schema - The Avro Schema
    */
  case class OutType(fqn: String, schema: String) extends EventType

  /***
    * ServiceMethod - a mapping from In to Future[Out]
    * @param in - InType
    * @param out - OutType
    */
  case class ServiceMethod(in: InType, out: OutType) extends TypeBus

  /***
    * ServiceDescriptor - fully describe a service
    * @param service - the name of the service
    * @param serviceMethods - a list of all the ServiceMethod
    */
  case class ServiceDescriptor(
                              service: String,
                              serviceMethods: Seq[ServiceMethod]
                              ) extends TypeBus

  /***
    * GetServiceDescriptor - a request to get the ServiceDescriptor
    * @param service - the name of the service that you want
    */
  case class GetServiceDescriptor(service: String) extends TypeBus

  /***
    * EventMeta - details and routing information for an Event
    * @param eventId - unique UUID of an event
    * @param eventType - the FQN of the event type
    * @param source - address of actor emitting the event
    * @param correlationId - id to correlate events
    * @param directReply - used by RPC actor and generated clients
    * @param userId - user id to associate this event with
    * @param socketId - web/tcp socket identifier
    * @param responseTo - the event UUID this is response to
    * @param extra - additional developer generated meta
    */
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

  /***
    * SocketEvent - event for passing data down a socket to a client.
    * @param meta - EventMeta
    * @param payload - Array[Byte] avro payload
    */
  final case class SocketEvent(
                             meta: EventMeta,
                             payload: Array[Byte]
                           ) extends TypeBus

  /***
    * PublishedEvent - wrapper for all events that go over the bus
    * @param meta - EventMeta
    * @param payload - Array[Byte] avro payload
    */
  final case class PublishedEvent(
                             meta: EventMeta,
                             payload: Array[Byte]
                              ) extends TypeBus

}






