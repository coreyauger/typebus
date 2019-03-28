package io.surfkit.typebus.bus.testkit

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import io.surfkit.typebus.actors.ProducerActor
import io.surfkit.typebus.bus._
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import java.util.UUID

import akka.actor.Actor.Receive
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe


trait TestConfig{
  import collection.JavaConversions._

  val cfg = ConfigFactory.load
}


class TypebusTestProducer(serviceId: ServiceIdentifier, system: ActorSystem, kafkaConfig: TestConfig = new TestConfig{} ) extends Publisher{
  import collection.JavaConversions._

  override def serviceIdentifier = serviceId

  val mediator = DistributedPubSub(system).mediator

  val publishActor = system.actorOf(
    Props(new Actor {
      import akka.cluster.pubsub.DistributedPubSubMediator.Publish
      def receive = {
        case event: PublishedEvent =>
          mediator ! Publish(event.meta.eventType.fqn, event, sendOneMessageToEachGroup=true )
      }
    }))


  def publish(event: PublishedEvent)(implicit system: ActorSystem): Unit = {
    try {
      publishActor ! event
      traceEvent(OutEventTrace(serviceIdentifier, event), event.meta)
    }catch{
      case t: Throwable =>
        produceErrorReport(t, event.meta)
    }
  }

  def busActor(implicit system: ActorSystem): ActorRef =
    system.actorOf(Props(new ProducerActor(this)))
}

class TypebusTestConsumer(sercieApi: Service, publisher: Publisher, system: ActorSystem, kafkaConfig: TestConfig = new TestConfig{}) extends Consumer{
  import system.dispatcher
  implicit val actorSystem = system

  val mediator = DistributedPubSub(system).mediator

  override def service = sercieApi
  val serviceDescription = service.makeServiceDescriptor
  val log = system.log
  log.info(
    s"""
       |********************************************************************************************************
       | << typebus Configuration <<<<<<<<<<<<<<<<<<<<<<<|>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
       |
       | akka.cluster.seed.zookeeper.url                  ${kafkaConfig.cfg.getString("akka.cluster.seed.zookeeper.url")}
       | kka.remote.netty.tcp.hostname                    ${kafkaConfig.cfg.getString("akka.remote.netty.tcp.hostname")}
       | akka.remote.netty.tcp.port                       ${kafkaConfig.cfg.getString("akka.remote.netty.tcp.port")}
       | akka.cluster.roles                               ${kafkaConfig.cfg.getStringList("akka.cluster.roles")}
       | bus.trace                                        ${kafkaConfig.cfg.getBoolean("bus.trace")}
       |********************************************************************************************************
       | serviceDescription:
       | ${serviceDescription}
       |********************************************************************************************************
    """.stripMargin)


  /***
    * getServiceDescriptor - default hander to broadcast ServiceDescriptions
    * @param x - GetServiceDescriptor is another service requesting a ServiceDescriptions
    * @param meta - EventMeta routing info
    * @return - Future[ServiceDescriptor]
    */
  def getServiceDescriptor(x: GetServiceDescriptor, meta: EventMeta): Future[ServiceDescriptor] = {
    system.log.debug(s"getServiceDescriptor: ${serviceDescription}")
    Future.successful(serviceDescription)
  }
  service.registerServiceStream(getServiceDescriptor _)



  system.log.info(s"\n\nTYPEBUS TEST STARTING TO LISTEN ON TOPICS: ${(service.serviceIdentifier.name :: (service.listOfFunctions.map(_._1.fqn) ::: service.listOfServiceFunctions.map(_._1.fqn))).mkString("\n")}")

  system.actorOf(Props(new Actor {
    (service.serviceIdentifier.name :: (service.listOfFunctions.map(_._1.fqn) ::: service.listOfServiceFunctions.map(_._1.fqn))).map { topic =>
      log.info(s"typebus akka bus subscribe to: ${topic}")
      mediator ! Subscribe(topic, Some(service.serviceIdentifier.name), context.self)
    }

    val retryState = scala.collection.mutable.HashMap.empty[String, (Int, RetryPolicy)]

    def receive: Receive = {
      case event: PublishedEvent =>
        (try {
          publisher.traceEvent(InEventTrace(service.serviceIdentifier, event), event.meta)
          consume(event).map{ ret =>
            implicit val timeout = Timeout(4 seconds)
            val retType: EventType = EventType.parse(ret.getClass.getCanonicalName)
            val publishedEvent = PublishedEvent(
              meta = event.meta.copy(
                eventId = UUID.randomUUID.toString,
                eventType = retType,
                responseTo = Some(event.meta.eventId),
                occurredAt = java.time.Instant.now
              ),
              payload = service.listOfServiceImplicitsWriters.get(retType).map{ writer =>
                writer.write(ret.asInstanceOf[TypeBus])
              }.getOrElse(service.listOfImplicitsWriters(retType).write(ret))
            )
            // RPC clients publish to the "Serivce Name" subscription, where that service then can route message back to RPC client.
            event.meta.directReply.filterNot(_.service.name == service.serviceIdentifier.name).foreach{ rpc =>
              publisher.publish( publishedEvent.copy(meta = publishedEvent.meta.copy(eventType = EventType.parse(rpc.service.name) )) )
            }
            publisher.publish(publishedEvent)
          }.recover{ case t: Throwable => Recoverable(t) }
        }catch{
          case t:Throwable => Future.successful(Recoverable(t))
        }).map{
          case Recoverable(t) =>
            publisher.produceErrorReport(t, event.meta, s"Error consuming event: ${event.meta.eventType}\n${t.getMessage}")
            val (attempt, retryPolicy): (Int,RetryPolicy) = retryState.get(event.meta.eventId).getOrElse {
              val p = service.streamBuilderMap(event.meta.eventType).retry.map { policy =>
                if (policy.isDefinedAt(t)) policy(t)
                else RetryPolicy.Fail
              }.getOrElse(RetryPolicy.Fail)
              retryState += event.meta.eventId -> (1, p)
              (1, p)
            }
            retryPolicy match{
              case RetryPolicy(numRetry, delay, backoff) if attempt >= numRetry =>
                retryState -= event.meta.eventId
              case RetryPolicy(_, delay, backoff) =>
                val timeout = backoff match{
                  case RetryBackoff.None => delay
                  case RetryBackoff.Linear => attempt * delay
                  case RetryBackoff.Exponential => (attempt*attempt) * delay
                }
                system.scheduler.scheduleOnce(timeout){
                  retryState += event.meta.eventId -> (attempt+1, retryPolicy)
                  context.self ! event // retry injecting upstream
                }
            }
          case _ =>
            retryState -= event.meta.eventId
        }
      case x =>
        log.warning(s"Got a message that was not expected: ${x}")
      }
  }))


  publisher.publish(serviceDescription)
}
