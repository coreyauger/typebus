package io.surfkit.typebus.module

import akka.Done
import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.sksamuel.avro4s.{AvroInputStream, AvroSchema}
import io.surfkit.typebus.event._
import io.surfkit.typebus.{AvroByteStreams, ByteStreamReader, ByteStreamWriter}
import java.util.UUID

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag

/***
  * The main type for defining your service.
  * @tparam UserBaseType - the base trait all your service types inherit from
  */
trait Service[UserBaseType] extends Module[UserBaseType] with AvroByteStreams{

  val publishedEventReader = new AvroByteStreamReader[PublishedEvent]

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
        eventType = x.getClass.getCanonicalName,
        responseTo = Some(meta.eventId)
      ),
      payload = writer.write(x))
      meta.directReply.foreach( system.actorSelection(_).resolveOne().map( actor => actor ! publishedEvent ) )
  }

  // TODO: macro to capture the types and add it to the service definition.
  //def publish[T <: UserBaseType : ClassTag](obj: T)(implicit reader: ByteStreamReader[T]) = {}

  def makeServiceDescriptor( serviceName: String ) = ServiceDescriptor(
    service = serviceName,
    serviceMethods = listOfFunctions.filterNot(_._2 == "scala.Unit").map{
      case (in, out) =>
        val reader = listOfImplicitsReaders(in)
        val writer = listOfImplicitsWriters(out)
        ServiceMethod(InType(in, reader.schema), OutType(out, writer.schema))
    }
  )
}
