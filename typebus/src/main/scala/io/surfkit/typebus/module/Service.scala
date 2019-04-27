package io.surfkit.typebus.module

import java.time.Instant

import akka.actor.{ActorLogging, ActorSystem}
import akka.util.Timeout
import io.surfkit.typebus.event._
import io.surfkit.typebus.{AvroByteStreams, ByteStreamReader, ByteStreamWriter}
import java.util.UUID

import io.surfkit.typebus.bus.{Publisher, StreamBuilder}

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
  */
abstract class Service(val serviceIdentifier: ServiceIdentifier, publisher: Publisher) extends Module with AvroByteStreams {

  val upTime = Instant.now()

  /***
    * registerStream - register a service level function that will also receive EventMeta
    * @param f - the function to register
    * @param reader - ByteStreamReader that knows how to convert Array[Byte] to a type T
    * @param writer - ByteStreamWriter that knows how to convert a type U to Array[Byte]
    * @tparam T - The IN service request type
    * @tparam U - The OUT service request type
    * @return - Unit
    */
  def registerStream[T <: Any : ClassTag, U <: Any : ClassTag](f:  (T, EventMeta) => Future[U]) (implicit reader: ByteStreamReader[T], writer: ByteStreamWriter[U]): StreamBuilder[T, U] =
    op2(funToPF2(f))

  /***
    * registerStream - register a sink
    * @param f - the function to register
    * @param reader - ByteStreamReader that knows how to convert Array[Byte] to a type T
    * @tparam T - The IN service request type
    * @return - Unit
    */
  def registerStream[T <: Any : ClassTag](f:  (T, EventMeta) => Future[Unit]) (implicit reader: ByteStreamReader[T]): StreamBuilder[T, Unit] =
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
  def registerServiceStream[T <: TypeBus : ClassTag, U <: TypeBus : ClassTag](f:  (T, EventMeta) => Future[U]) (implicit reader: ByteStreamReader[T], writer: ByteStreamWriter[U]): StreamBuilder[T, U] =
    op2Service(funToPF2(f))


  /***
    * Route a message to the proper RPC client to close the Future response.
    * @param x - rpc reply event
    * @param system - Actor system
    */
  def handleRpcReply( x: PublishedEvent )(implicit system: ActorSystem): Future[Unit] = {
    import system.dispatcher
    x.meta.directReply.filterNot(_.service.name == serviceIdentifier.name).map{rpc =>
      system.actorSelection(rpc.path).resolveOne(4 seconds).map{
        actor => actor ! x
      }
    }.getOrElse(Future.successful( Unit ))
  }

  def makeServiceDescriptor = ServiceDescriptor(
    service = serviceIdentifier,
    upTime = upTime,
    serviceMethods = listOfFunctions.toList.filterNot(_._2 == EventType.parse("scala.Unit")).map{
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
