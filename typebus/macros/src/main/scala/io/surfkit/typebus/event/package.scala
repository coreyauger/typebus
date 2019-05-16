package io.surfkit.typebus

import java.time.Instant
import java.util.UUID

package object event {

  /***
    * TypeBus - base type
    */
  sealed trait TypeBus{}

  final case class ServiceIdentifier(name: String, id: UUID = UUID.randomUUID) extends TypeBus

  trait Trace extends TypeBus{
    def service: ServiceIdentifier
    def event: PublishedEvent
  }
  case class ServiceException(
               message: String,
               throwableType: String,
               stackTrace: Seq[String],
               extra: Map[String, String] = Map.empty
  ) extends TypeBus

  case class InEventTrace(
               service: ServiceIdentifier,
               event: PublishedEvent) extends Trace

  case class OutEventTrace(
                 service: ServiceIdentifier,
                 event: PublishedEvent) extends Trace
  case class ExceptionTrace(
                 service: ServiceIdentifier,
                 event: PublishedEvent
               ) extends Trace

  /***
    *  DtoType - wrap the Fully Qualified Name of a type
    */
  trait DtoType{
    /***
      * fqn - Fully Qualified Name
      * @return - return the fully qualified name of a type.
      */
    def fqn: String
  }

  case class EventType(fqn: String) extends DtoType
  object EventType{
    def parse(et: String): EventType = {
      if (et.startsWith("api.")) EventType(et.replaceFirst("api.", ""))
      else EventType(et)
    }
    def unit = EventType.parse("scala.Unit")
  }

  /***
    * InType - This is the type passed IN to a service method
    * @param fqn - Fully Qualified Name of Type
    * @param schema - The Avro(or other) Schema
    */
  case class InType(fqn: String) extends DtoType

  /***
    * OutType - This is the type passed OUT of a service function
    * @param fqn - Fully Qualified Name of Type
    * @param schema - The Avro(or other) Schema
    */
  case class OutType(fqn: String) extends DtoType

  /***
    * Store the Fqn and the schema for the type.  The fqn can serve as a lookup for that types schema
    * @param fqn - Fully Qualified Name of Type
    * @param schema - The Avro(or other) Schema
    */
  case class TypeSchema(`type`: EventType, schema: String) extends TypeBus

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
                              service: ServiceIdentifier,
                              upTime: Instant,
                              serviceMethods: Seq[ServiceMethod],
                              types: Map[String, TypeSchema]
                              ) extends TypeBus

  /***
    * GetServiceDescriptor - a request to get the ServiceDescriptor
    * @param service - the name of the service that you want
    */
  case class GetServiceDescriptor(service: String) extends TypeBus

  /***
    * RpcClient - used in EventMeta to make a direct reply to an RPC client
    * @param path - the actor path of the RPC client request
    * @param service - the service Identifier for the RPC client
    */
  case class RpcClient(path: String, service: ServiceIdentifier) extends TypeBus

  /***
    * EventMeta - details and routing information for an Event
    * @param eventId - unique UUID of an event
    * @param eventType - the FQN of the event type
    * @param correlationId - id to correlate events
    * @param directReply - used by RPC actor and generated clients
    * @param key - the key to use for kafka partition
    * @param socketId - web/tcp socket identifier
    * @param responseTo - the event UUID this is response to
    * @param extra - additional developer generated meta
    */
  case class EventMeta(eventId: String,
                       eventType: EventType,
                       correlationId: Option[String],
                       trace: Boolean = false,
                       directReply: Option[RpcClient] = None,
                       key: Option[String] = None,
                       socketId: Option[String] = None,
                       responseTo: Option[String] = None,
                       extra: Map[String, String] = Map.empty,
                       occurredAt: Instant = Instant.now()) extends TypeBus

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

  final case class Recoverable(cause: Throwable) extends TypeBus

  final case class Hb(ts: Long) extends TypeBus

}






