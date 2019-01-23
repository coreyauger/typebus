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

  //implicit val system = context.system
  val cfg = ConfigFactory.load
  import context.dispatcher
  val trace = cfg.getBoolean("bus.trace")
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


  def traceEvent( f: (ServiceIdentifier) => Trace, meta: EventMeta): Unit = {
    if(  // CA - well this is lame :(
      (trace || meta.trace) &&
        !meta.eventType.fqn.endsWith(InEventTrace.getClass.getSimpleName.replaceAllLiterally("$","")) &&
        !meta.eventType.fqn.endsWith(OutEventTrace.getClass.getSimpleName.replaceAllLiterally("$","")) &&
        !meta.eventType.fqn.endsWith(ExceptionTrace.getClass.getSimpleName.replaceAllLiterally("$",""))
       ){
        val event = f( ServiceIdentifier(service.serviceName, service.serviceId) )
        publishActor ! PublishedEvent(
            meta = meta.copy(
              eventId = UUID.randomUUID().toString,
              eventType = event.getClass.getCanonicalName,
              correlationId = None,
              trace = false,
              occurredAt = DateTime.now
            ),
            payload = event match{
              case x: OutEventTrace => service.OutEventTraceWriter.write(x)
              case x: InEventTrace => service.InEventTraceWriter.write(x)
              case x: ExceptionTrace => service.ExceptionTraceWriter.write(x)
            }
          )
    }
  }

  def publish(event: PublishedEvent): Unit = {
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
              occurredAt = DateTime.now()
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
          t.printStackTrace()
          val sw = new StringWriter
          t.printStackTrace(new PrintWriter(sw))
          val ex = ServiceException(
            message = "typebus caught exception",
            stackTrace = sw.toString.split("\n").toSeq
          )
          traceEvent( { s: ServiceIdentifier =>
            ExceptionTrace(s.service, s.serviceId, PublishedEvent(
              meta = EventMeta(
                eventId = UUID.randomUUID().toString,
                source = "",
                eventType = ex.getClass.getCanonicalName,
                correlationId = None,
                trace = true
              ),
              payload = ServiceExceptionWriter.write(ex)
            ))
          }, event.meta)
          context.system.log.error("typebus caught exception",t)
      }
  }
}
