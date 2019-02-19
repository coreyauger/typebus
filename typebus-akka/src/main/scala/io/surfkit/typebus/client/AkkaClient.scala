package io.surfkit.typebus.client

import java.io.{PrintWriter, StringWriter}
import java.nio.ByteBuffer
import java.util.UUID

import com.typesafe.config.ConfigFactory
import akka.actor.{ActorRef, ActorSystem, Props, Actor}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import io.surfkit.typebus.{AvroByteStreams, ByteStreamReader, ByteStreamWriter}
import io.surfkit.typebus.actors.GatherActor
import io.surfkit.typebus.bus.Publisher
import io.surfkit.typebus.event._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

/***
  * Client - base class for generating RPC clients
  * @param system - akka actor system
  */
class AkkaClient(service: ServiceIdentifier)(implicit system: ActorSystem) extends Client with Publisher with AvroByteStreams {
  import akka.pattern.ask
  import akka.util.Timeout
  import system.dispatcher

  val cfg = ConfigFactory.load
  val tyebusMap = scala.collection.mutable.Map.empty[String, ActorRef]

  val mediator = DistributedPubSub(system).mediator

  val publishActor = system.actorOf(
    Props(new Actor {
      import akka.cluster.pubsub.DistributedPubSubMediator.Publish
      def receive = {
        case event: PublishedEvent => mediator ! Publish(event.meta.eventType.fqn, event, sendOneMessageToEachGroup=true )
      }
    }))

  def publish(event: PublishedEvent)(implicit system: ActorSystem): Unit =
    try {
      system.log.info(s"[KinesisClient] publish ${event.meta.eventType}")
      publishActor ! event
    }catch{
      case e:Exception =>
        system.log.error(e, "Error trying to publish event.")
    }

  def busActor(implicit system: ActorSystem): ActorRef =
    publishActor

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
  def wire[T : ClassTag, U : ClassTag](x: T, eventMeta: Option[EventMeta] = None)(implicit timeout:Timeout = Timeout(4 seconds), w:ByteStreamWriter[T], r: ByteStreamReader[U]) :Future[U] = {
    val tType = scala.reflect.classTag[T].runtimeClass.getCanonicalName
    val uType = scala.reflect.classTag[U].runtimeClass.getCanonicalName
    val gather = system.actorOf(Props(new GatherActor[T, U](service, this, timeout, w, r)))
    val meta = eventMeta.map(_.copy(eventId = UUID.randomUUID().toString, eventType = EventType.parse(x.getClass.getCanonicalName))).getOrElse{
      EventMeta(
        eventId = UUID.randomUUID().toString,
        eventType = EventType.parse(x.getClass.getCanonicalName),
        source = "",
        directReply = None,
        correlationId = None
      )
    }
    (gather ? GatherActor.Request(x)).map(_.asInstanceOf[U]).recoverWith{
      case t: Throwable =>
        produceErrorReport(service)(t, meta, s"FAILED RPC call ${tType} => Future[${uType}] failed with exception '${t.getMessage}'")
        Future.failed(t)
    }
  }
}


