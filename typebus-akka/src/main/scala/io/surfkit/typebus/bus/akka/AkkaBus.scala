package io.surfkit.typebus.bus.akka

import java.io.{PrintWriter, StringWriter}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}

import scala.concurrent.Future
import scala.concurrent.duration._
import com.typesafe.config.{ConfigException, ConfigFactory}
import io.surfkit.typebus.AvroByteStreams
import io.surfkit.typebus.actors.ProducerActor
import io.surfkit.typebus.bus.Bus
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import java.util.UUID

import org.joda.time.DateTime

import scala.util.Try

trait AkkaBus[UserBaseType] extends Bus[UserBaseType] with AvroByteStreams with Actor with ActorLogging {
  service: Service[UserBaseType] =>

  implicit val system = context.system
  val cfg = ConfigFactory.load
  import context.dispatcher
  log.info(
    s"""
      |********************************************************************************************************
      | << typebus Configuration <<<<<<<<<<<<<<<<<<<<<<<|>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
      |
      | akka.cluster.seed.zookeeper.url                  ${cfg.getString("akka.cluster.seed.zookeeper.url")}
      | kka.remote.netty.tcp.hostname                    ${cfg.getString("akka.remote.netty.tcp.hostname")}
      | akka.remote.netty.tcp.port                       ${cfg.getString("akka.remote.netty.tcp.port")}
      | akka.cluster.roles                               ${cfg.getStringList("akka.cluster.roles")}
      | bus.trace                                        ${cfg.getBoolean("bus.trace")}
      |********************************************************************************************************
    """.stripMargin)

  val mediator = DistributedPubSub(context.system).mediator

  val publishActor = context.actorOf(
    Props(new Actor {
      import akka.cluster.pubsub.DistributedPubSubMediator.Publish
      def receive = {
        case event: PublishedEvent => mediator ! Publish(event.meta.eventType.fqn, event, sendOneMessageToEachGroup=true )
      }
    }))

  def publish(event: PublishedEvent)(implicit system: ActorSystem): Unit = {
    publishActor ! event
    traceEvent( { s: ServiceIdentifier =>
      OutEventTrace(s.service, s.serviceId, event)
    }, event.meta)
  }

  def busActor(implicit system: ActorSystem): ActorRef =
    context.actorOf(Props(new ProducerActor(this)))

  def startTypeBus(implicit system: ActorSystem): Unit = {
    val serviceDescription = makeServiceDescriptor(serviceName)
    /***
      * getServiceDescriptor - default hander to broadcast ServiceDescriptions
      * @param x - GetServiceDescriptor is another service requesting a ServiceDescriptions
      * @param meta - EventMeta routing info
      * @return - Future[ServiceDescriptor]
      */
    def getServiceDescriptor(x: GetServiceDescriptor, meta: EventMeta): Future[ServiceDescriptor] = {
      context.system.log.debug(s"getServiceDescriptor: ${serviceDescription}")
      Future.successful(serviceDescription)
    }
    registerServiceStream(getServiceDescriptor _)

    (listOfFunctions.map(_._1) ::: listOfServiceFunctions.map(_._1)).map { topic =>
      log.info(s"typebus akka bus subscribe to: ${topic}")
      mediator ! Subscribe(topic.fqn, Some(service.serviceIdentifier.service), context.self)
    }
  }

  def receive: Receive = {
    case event: PublishedEvent =>
      context.system.log.info(s"TypeBus: got msg for topic: ${event.meta.eventType}")
      try {
        traceEvent({ s: ServiceIdentifier =>
          InEventTrace(s.service, s.serviceId, event)
        }, event.meta)
        consume(event) map{ ret =>
          implicit val timeout = Timeout(4 seconds)
          val retType: EventType = EventType.parse(ret.getClass.getCanonicalName)
          val publishedEvent = PublishedEvent(
            meta = event.meta.copy(
              eventId = UUID.randomUUID.toString,
              eventType = retType,
              responseTo = Some(event.meta.eventId),
              occurredAt = java.time.Instant.now
            ),
            payload = listOfServiceImplicitsWriters.get(retType).map{ writer =>
              writer.write(ret.asInstanceOf[TypeBus])
            }.getOrElse(listOfImplicitsWriters(retType).write(ret.asInstanceOf[UserBaseType]))
          )
          event.meta.directReply.foreach( context.system.actorSelection(_).resolveOne().map( actor => actor ! publishedEvent ) )
          publish(publishedEvent)
        }
      }catch{
        case t:Throwable =>
          val error = s"Error consuming event: ${publish.meta.eventType}\n${t.getMessage}"
          produceErrorReport(t, event.meta, error)
      }
  }
}
