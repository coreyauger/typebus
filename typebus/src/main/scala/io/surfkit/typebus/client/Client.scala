package io.surfkit.typebus.client

import io.surfkit.typebus.{ByteStreamReader, ByteStreamWriter}
import io.surfkit.typebus.event.EventMeta
import io.surfkit.typebus.bus.Publisher
import io.surfkit.typebus.actors.GatherActor
import io.surfkit.typebus.event._
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.actor.{ActorSystem, Props}
import scala.reflect.ClassTag
import akka.util.Timeout
import java.util.UUID
/***
  * Client - base class for generating RPC clients
  * @param system - akka actor system
  */
class Client(serviceIdentifier: ServiceIdentifier, publisher: Publisher, system: ActorSystem){
  import system.dispatcher
  import akka.pattern.ask
  /***
    * wire - function to create a Request per Actor and perform the needed type conversions.
    * @param x - This is the request type.  This is of type T.
    * @param timeout - configurable actor timeout with default of 4 seconds
    * @param w - ByteStreamWriter (defaults to avro)
    * @param r - ByteStreamReader (defaults to avro)
    * @tparam T - The IN type for the service call
    * @param eventMeta - The Event Meta from any previous typebus calls to thread through the request
    * @tparam U - The OUT type in the service called. Wrapped as Future[U]
    * @return - The Future[U] return from the service call.
    */
  def wire[T : ClassTag, U : ClassTag](x: T, eventMeta: Option[EventMeta] = None)(implicit timeout:Timeout = Timeout(4 seconds), w:ByteStreamWriter[T], r: ByteStreamReader[U]) :Future[Either[ServiceException,U]]= {
    val tType = scala.reflect.classTag[T].runtimeClass.getCanonicalName
    val uType = scala.reflect.classTag[U].runtimeClass.getCanonicalName
    val gather = system.actorOf(Props(new GatherActor[T, U](serviceIdentifier, publisher, timeout, w, r)))
    val meta = eventMeta.map(_.copy(eventId = UUID.randomUUID().toString, eventType = EventType.parse(x.getClass.getCanonicalName))).getOrElse{
      EventMeta(
        eventId = UUID.randomUUID().toString,
        eventType = EventType.parse(x.getClass.getCanonicalName),
        source = "",
        directReply = None,
        correlationId = None
      )
    }
    (gather ? GatherActor.Request(x)).map{
      case x: U => Right(x.asInstanceOf[U])
      case y: ServiceException => Left(y)
    }.recoverWith{
      case t: Throwable =>
        publisher.produceErrorReport(t, meta, s"FAILED RPC call ${tType} => Future[${uType}] failed with exception '${t.getMessage}'")(system)
        Future.failed(t)
    }
  }
}


