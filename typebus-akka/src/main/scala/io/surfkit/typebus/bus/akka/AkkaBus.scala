package io.surfkit.typebus.bus.akka

import java.io.{PrintWriter, StringWriter}

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.util.Timeout
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}

import scala.concurrent.Future
import scala.concurrent.duration._
import com.typesafe.config.{ConfigException, ConfigFactory}
import io.surfkit.typebus.{AvroByteStreams, event}
import io.surfkit.typebus.actors.ProducerActor
import io.surfkit.typebus.bus.{Consumer, Publisher}
import io.surfkit.typebus.module.{Module, Service}
import java.util.UUID

import io.surfkit.typebus.event.{EventMeta, EventType, GetServiceDescriptor, InEventTrace, OutEventTrace, PublishedEvent, Recoverable, ServiceDescriptor, ServiceIdentifier, TypeBus}
import org.joda.time.DateTime

import scala.util.Try

class AkkaBusProducer(serviceId: ServiceIdentifier, sys: ActorSystem) extends Publisher{
  implicit val system = sys
  val log = system.log
  val cfg = ConfigFactory.load

  override def serviceIdentifier = serviceId

  val mediator = DistributedPubSub(system).mediator

  val publishActor = system.actorOf(
    Props(new Actor {
      import akka.cluster.pubsub.DistributedPubSubMediator.Publish
      def receive = {
        case event: PublishedEvent =>
          system.log.info(s"AkkaBusProducer publish[${event.meta.eventType}] to mediator: ${mediator}")
          def handleRpcCallback = List(event.meta).find(_.responseTo.isDefined).flatMap(_.directReply.map(_.service.name))
          mediator ! Publish(event.meta.eventType.fqn, event, sendOneMessageToEachGroup=true )
          handleRpcCallback.foreach{ serviceName =>
            system.log.info(s"publish[${event.meta.eventType}] to rpc channel:${serviceName}")
            mediator ! Publish(serviceName, event)
          }
      }
    }))

  def publish(event: PublishedEvent)(implicit system: ActorSystem): Unit = {
    publishActor ! event
    traceEvent(OutEventTrace(serviceIdentifier, event), event.meta)
  }

  def busActor(implicit system: ActorSystem): ActorRef =
    system.actorOf(Props(new ProducerActor(this)))
}

class AkkaBusConsumer(sercieApi: Service, publisher: Publisher, sys: ActorSystem) extends Actor with Consumer
{
  implicit val system = sys
  import system.dispatcher
  val log = system.log
  val cfg = ConfigFactory.load
  val mediator = DistributedPubSub(system).mediator

  override def service = sercieApi

  log.info(
    s"""
       |********************************************************************************************************
       | << typebus Configuration <<<<<<<<<<<<<<<<<<<<<<<|>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
       |
       | kka.remote.netty.tcp.hostname                    ${cfg.getString("akka.remote.netty.tcp.hostname")}
       | akka.remote.netty.tcp.port                       ${cfg.getString("akka.remote.netty.tcp.port")}
       | akka.cluster.roles                               ${cfg.getStringList("akka.cluster.roles")}
       | bus.trace                                        ${cfg.getBoolean("bus.trace")}
       |********************************************************************************************************
    """.stripMargin)

  /***
    * getServiceDescriptor - default handler to broadcast ServiceDescriptions
    * @param x - GetServiceDescriptor is another service requesting a ServiceDescriptions
    * @param meta - EventMeta routing info
    * @return - Future[ServiceDescriptor]
    */
  def getServiceDescriptor(x: GetServiceDescriptor, meta: EventMeta): Future[ServiceDescriptor] = {
    system.log.debug(s"getServiceDescriptor: ${sercieApi.makeServiceDescriptor}")
    Future.successful(sercieApi.makeServiceDescriptor)
  }
  service.registerServiceStream(getServiceDescriptor _)
  system.log.info(s"\n\nTYPEBUS AKKA STARTING TO LISTEN ON TOPICS: ${(service.serviceIdentifier.name :: (service.listOfFunctions.keys.map(_.fqn).toList ::: service.listOfServiceFunctions.keys.map(_.fqn).toList)).mkString("\n")}")

  (service.serviceIdentifier.name :: (service.listOfFunctions.keys.map(_.fqn).toList ::: service.listOfServiceFunctions.keys.map(_.fqn).toList)).map { topic =>
    log.info(s"typebus akka bus subscribe to: ${topic}")
    mediator ! Subscribe(topic, Some(service.serviceIdentifier.name), context.self)
  }

  def receive: Receive = {
    case msg: PublishedEvent =>
      system.log.info(s"AkkaBusConsumer got[${msg.meta.eventType}]")
      context.system.log.info(s"TypeBus: got msg for topic: ${msg.meta.eventType}")
      try {
        publisher.traceEvent(InEventTrace(service.serviceIdentifier, msg), msg.meta)
        consume(msg).map{ret =>
          context.system.log.info(s"TypeBus: RET: ${EventType.parse(ret.getClass.getCanonicalName)}")
          println(s"listOfImplicitsWriters: ${service.listOfImplicitsWriters}")
          val retType: EventType = EventType.parse(ret.getClass.getCanonicalName)
          val publishedEvent = PublishedEvent(
            meta = msg.meta.copy(
              eventId = UUID.randomUUID.toString,
              eventType = retType,
              occurredAt = java.time.Instant.now
            ),
            payload = service.listOfServiceImplicitsWriters.get(retType).map{ writer =>
              writer.write(ret.asInstanceOf[TypeBus])
            }.getOrElse(service.listOfImplicitsWriters(retType).write(ret))
          )
          context.system.log.info(s"** TypeBus: RET PUBLISH: ${publishedEvent}")
          publisher.publish(publishedEvent)
          (msg, msg, ret, msg.meta.eventType)
        }
      }catch{
        case t:Throwable =>
          log.error(s"Error consuming event: ${msg.meta.eventType}", t)
          publisher.produceErrorReport(t, msg.meta, s"Error consuming event: ${msg.meta.eventType}\n${t.getMessage}")
      }
  }

}

/*
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
          event.meta.directReply.filterNot(_.service.service == serviceName).foreach( rpc => system.actorSelection(rpc.path).resolveOne().map( actor => actor ! publishedEvent ) )
          publish(publishedEvent)
        }
      }catch{
        case t:Throwable =>
          val error = s"Error consuming event: ${event.meta.eventType}\n${t.getMessage}"
          produceErrorReport(t, event.meta, error)
      }
  }
}
*/