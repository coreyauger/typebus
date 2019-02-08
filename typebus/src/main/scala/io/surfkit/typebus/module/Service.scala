package io.surfkit.typebus.module

import java.time.Instant

import akka.actor.{ActorLogging, ActorSystem}
import akka.util.Timeout
import io.surfkit.typebus.event._
import io.surfkit.typebus.{AvroByteStreams, ByteStreamReader, ByteStreamWriter}
import java.util.UUID

import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag

object Service{
  val registry = scala.collection.mutable.HashMap.empty[ EventType, String]

  def registerServiceType[T : ClassTag](serviceType: io.surfkit.typebus.Schemacha, fqn: String) = {

    val runtimeClass = scala.reflect.classTag[T].runtimeClass
    println(s"\n\nruntimeClass: ${runtimeClass}")
    // CA - pretty cheesy data store.
    registry += EventType.parse(fqn) -> serviceType.schema
  }
}

/***
  * The main type for defining your service.
  * @tparam UserBaseType - the base trait all your service types inherit from
  */
abstract class Service[UserBaseType](val serviceName: String) extends Module[UserBaseType] with AvroByteStreams {

  val upTime = Instant.now()
  val serviceId = UUID.randomUUID().toString
  val serviceIdentifier = ServiceIdentifier(serviceName, serviceId)

  /***
    * registerStream - register a service level function
    * @param f - the function to register
    * @param reader - ByteStreamReader that knows how to convert Array[Byte] to a type T
    * @param writer - ByteStreamWriter that knows how to convert a type U to Array[Byte]
    * @tparam T - The IN service request type
    * @tparam U - The OUT service request type
    * @return - Unit
    */
  def registerStream[T <: UserBaseType : ClassTag, U <: UserBaseType : ClassTag](f: (T) => Future[U]) (implicit reader: ByteStreamReader[T], writer: ByteStreamWriter[U]) =
    op(funToPF(f))

  /***
    * registerStream - register a service level function that will also receive EventMeta
    * @param f - the function to register
    * @param reader - ByteStreamReader that knows how to convert Array[Byte] to a type T
    * @param writer - ByteStreamWriter that knows how to convert a type U to Array[Byte]
    * @tparam T - The IN service request type
    * @tparam U - The OUT service request type
    * @return - Unit
    */
  def registerStream[T <: UserBaseType : ClassTag, U <: UserBaseType : ClassTag](f:  (T, EventMeta) => Future[U])  (implicit reader: ByteStreamReader[T], writer: ByteStreamWriter[U]) =
    op2(funToPF2(f))

  /***
    * registerStream - register a sink
    * @param f - the function to register
    * @param reader - ByteStreamReader that knows how to convert Array[Byte] to a type T
    * @tparam T - The IN service request type
    * @return - Unit
    */
  def registerStream[T <: UserBaseType : ClassTag](f:  (T, EventMeta) => Future[Unit])  (implicit reader: ByteStreamReader[T]) =
    op2Unit(funToPF2Unit(f))

  /***
    * registerServiceStream - register a hidden typebus level service function
    * @param f - the function to register
    * @param reader - ByteStreamReader that knows how to convert Array[Byte] to a type T
    * @param writer - ByteStreamWriter that knows how to convert a type U to Array[Byte]
    * @tparam T - The IN service request type
    * @tparam U - The OUT service request type
    * @return - Unit
    */
  def registerServiceStream[T <: TypeBus : ClassTag, U <: TypeBus : ClassTag](f:  (T, EventMeta) => Future[U])  (implicit reader: ByteStreamReader[T], writer: ByteStreamWriter[U]) =
    op2Service(funToPF2(f))


  /***
    * replyToSender - target an actor for the reply of a message
    * @param meta - EventMeta used to help with the routing
    * @param x - The resonse of type U that needs to be sent to the actor.
    * @param system - The akka actor system
    * @param writer - ByteStreamWriter that knows how to convert a type U to Array[Byte]
    * @param ex - Execution context
    * @tparam U - The type of the response
    */
  def replyToSender[U <: UserBaseType](meta: EventMeta, x: U)(implicit system: ActorSystem, writer: ByteStreamWriter[U], ex: ExecutionContext) = {
    implicit val timeout = Timeout(4 seconds)
    val publishedEvent = PublishedEvent(
      meta = meta.copy(
        eventId = UUID.randomUUID.toString,
        eventType = EventType.parse(x.getClass.getCanonicalName),
        responseTo = Some(meta.eventId)
      ),
      payload = writer.write(x))
      meta.directReply.foreach( system.actorSelection(_).resolveOne().map( actor => actor ! publishedEvent ) )
  }

  def makeServiceDescriptor( serviceName: String ) = ServiceDescriptor(
    service = serviceName,
    serviceId = serviceId,
    upTime = upTime,
    serviceMethods = listOfFunctions.filterNot(_._2 == EventType.parse("scala.Unit")).map{
      case (in, out) =>
        val reader = listOfImplicitsReaders(in)
        val writer = listOfImplicitsWriters(out)
        Service.registry += in -> reader.schema
        Service.registry += out -> writer.schema
        ServiceMethod(InType(in.fqn), OutType(out.fqn))
    },
    types = Service.registry.map{
      case (fqn, schema) => fqn.fqn -> TypeSchema(fqn, schema)
    }.toMap
  )
}
