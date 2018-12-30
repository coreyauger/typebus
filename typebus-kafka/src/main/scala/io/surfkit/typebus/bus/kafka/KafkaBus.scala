package io.surfkit.typebus.bus.kafka

import java.io.{PrintWriter, StringWriter}

import akka.Done
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import io.surfkit.typebus.actors.ProducerActor
import io.surfkit.typebus.bus.Bus
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.util.UUID
import java.time.Instant

import org.joda.time.DateTime

trait KafkaBus[UserBaseType] extends Bus[UserBaseType] {
  service: Service[UserBaseType] =>

  val cfg = ConfigFactory.load
  val kafka = cfg.getString("bus.kafka")
  val trace = cfg.getBoolean("bus.trace")

  import collection.JavaConversions._
  val producer = new KafkaProducer[Array[Byte], Array[Byte]](Map(
    "bootstrap.servers" -> kafka,
    "key.serializer" ->  "org.apache.kafka.common.serialization.ByteArraySerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer"
  ))

  def traceEvent( f:(ServiceIdentifier) => Trace, meta: EventMeta): Unit = {
    if(  // CA - well this is lame :(
      (trace || meta.trace) &&
        !meta.eventType.endsWith(InEventTrace.getClass.getSimpleName.replaceAllLiterally("$","")) &&
        !meta.eventType.endsWith(OutEventTrace.getClass.getSimpleName.replaceAllLiterally("$","")) &&
        !meta.eventType.endsWith(ExceptionTrace.getClass.getSimpleName.replaceAllLiterally("$",""))
    ){
      val event = f(ServiceIdentifier(serviceName, serviceId))
      producer.send(
        new ProducerRecord[Array[Byte], Array[Byte]](
          event.getClass.getCanonicalName,
          service.publishedEventWriter.write(PublishedEvent(
            meta = meta.copy(
              eventId = UUID.randomUUID().toString,
              eventType = event.getClass.getCanonicalName,
              correlationId = None,
              trace = false,
              occurredAt = Instant.now
            ),
            payload = event match{
              case x: OutEventTrace => service.OutEventTraceWriter.write(x)
              case x: InEventTrace => service.InEventTraceWriter.write(x)
              case x: ExceptionTrace => service.ExceptionTraceWriter.write(x)
            }
          ))
        )
      )
    }
  }

  def publish(event: PublishedEvent): Unit = {
    try {
      producer.send(
        new ProducerRecord[Array[Byte], Array[Byte]](
          event.meta.eventType,
          service.publishedEventWriter.write(event)
        )
      )
      traceEvent({ s: ServiceIdentifier =>
        OutEventTrace(s.service, s.serviceId, event)
      }, event.meta)
    }catch{
      case e:Exception =>
        e.printStackTrace()
        // TODO: trace.. but how do we avoid getting stuck in a loop?
    }
  }

  def busActor(implicit system: ActorSystem): ActorRef =
    system.actorOf(Props(new ProducerActor(this)))

  def startTypeBus(implicit system: ActorSystem): Unit = {
    import system.dispatcher
    val serviceDescription = makeServiceDescriptor(serviceName)
    val log = system.log
    log.info(
      s"""
         |********************************************************************************************************
         | << typebus Configuration <<<<<<<<<<<<<<<<<<<<<<<|>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
         |
         | akka.cluster.seed.zookeeper.url                  ${cfg.getString("akka.cluster.seed.zookeeper.url")}
         | kka.remote.netty.tcp.hostname                    ${cfg.getString("akka.remote.netty.tcp.hostname")}
         | akka.remote.netty.tcp.port                       ${cfg.getString("akka.remote.netty.tcp.port")}
         | akka.cluster.roles                               ${cfg.getStringList("akka.cluster.roles")}
         | bus.kafka                                        ${cfg.getString("bus.kafka")}
         | bus.trace                                        ${cfg.getBoolean("bus.trace")}
         |********************************************************************************************************
    """.stripMargin)

    val decider: Supervision.Decider = {
      case _ => Supervision.Resume  // Never give up !
    }
    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(kafka)
      .withGroupId(serviceName)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

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
    registerServiceStream(getServiceDescriptor _)


    val replyAndCommit = new PartialFunction[(ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent, Any), Future[Done]]{
      def apply(x: (ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent, Any) ) = {
        system.log.debug("******** TypeBus: replyAndCommit")
        system.log.debug(s"listOfImplicitsWriters: ${listOfImplicitsWriters}")
        system.log.debug(s"type: ${x._3.getClass.getCanonicalName}")
        if(x._3 != Unit) {
          implicit val timeout = Timeout(4 seconds)
          val retType = x._3.getClass.getCanonicalName
          val publishedEvent = PublishedEvent(
            meta = x._2.meta.copy(
              eventId = UUID.randomUUID.toString,
              eventType = x._3.getClass.getCanonicalName,
              responseTo = Some(x._2.meta.eventId),
              occurredAt = Instant.now()
            ),
            payload = listOfServiceImplicitsWriters.get(retType).map{ writer =>
              writer.write(x._3.asInstanceOf[TypeBus])
            }.getOrElse(listOfImplicitsWriters(retType).write(x._3.asInstanceOf[UserBaseType]))
          )
          x._2.meta.directReply.foreach( system.actorSelection(_).resolveOne().map( actor => actor ! publishedEvent ) )
          publish(publishedEvent)
        }
        system.log.debug("committableOffset !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        x._1.committableOffset.commitScaladsl()
      }
      def isDefinedAt(x: (ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent, Any) ) = true
    }

    system.log.debug(s"STARTING TO LISTEN ON TOPICS:\n ${listOfFunctions.map(_._1)}")

    Consumer.committableSource(consumerSettings, Subscriptions.topics( (listOfFunctions.map(_._1) ::: listOfServiceFunctions.map(_._1)) :_*))
      .mapAsyncUnordered(4) { msg =>
        system.log.debug(s"TypeBus: got msg for topic: ${msg.record.topic()}")
        val publish = service.publishedEventReader.read(msg.record.value())
        try {
          traceEvent({ s: ServiceIdentifier =>
            InEventTrace(s.service, s.serviceId, publish)
          }, publish.meta)
          consume(publish).map(x => (msg, publish, x))
        }catch{
          case t:Throwable =>
            system.log.error(t.getMessage,t)
            val sw = new StringWriter
            t.printStackTrace(new PrintWriter(sw))
            val ex = ServiceException(
              message = t.getMessage,
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
            }, publish.meta)
            throw t
        }
      }
      .mapAsyncUnordered(4)(replyAndCommit)
      .runWith(Sink.ignore)

    publish(makeServiceDescriptor(serviceName))
  }
}
