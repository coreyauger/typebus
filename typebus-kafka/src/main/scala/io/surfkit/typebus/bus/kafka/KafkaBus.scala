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
import io.surfkit.typebus.bus._
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.util.UUID
import java.time.Instant


trait TypebusKafkaConfig{
  import collection.JavaConversions._

  val cfg = ConfigFactory.load
  val kafkaConfig: com.typesafe.config.ConfigObject = cfg.getObject("bus.kafka")
  val kafkaConfigMap = (for {
    entry : java.util.Map.Entry[String, com.typesafe.config.ConfigValue] <- kafkaConfig.entrySet()
    key = entry.getKey.replaceAll("-",".")
    value = entry.getValue.unwrapped().toString
  } yield (key, value)).toMap


}


class TypebusKafkaProducer(serviceId: ServiceIdentifier, system: ActorSystem, kafkaConfig: TypebusKafkaConfig = new TypebusKafkaConfig{} ) extends Publisher{
  import collection.JavaConversions._

  override def serviceIdentifier = serviceId

  val producer = new KafkaProducer[Array[Byte], Array[Byte]](Map(
    "key.serializer" ->  "org.apache.kafka.common.serialization.ByteArraySerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer"
  ) ++ kafkaConfig.kafkaConfigMap )

  def publish(event: PublishedEvent)(implicit system: ActorSystem): Unit = {
    try {
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](event.meta.eventType.fqn, publishedEventWriter.write(event)))
      traceEvent(OutEventTrace(serviceIdentifier, event), event.meta)
    }catch{
      case t: Throwable =>
        produceErrorReport(t, event.meta)
    }
  }

  def busActor(implicit system: ActorSystem): ActorRef =
    system.actorOf(Props(new ProducerActor(this)))
}

class TypebusKafkaConsumer(sercieApi: Service, publisher: Publisher, system: ActorSystem, kafkaConfig: TypebusKafkaConfig = new TypebusKafkaConfig{}) extends Consumer{
  import system.dispatcher
  implicit val actorSystem = system

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
       | bus.kafka                                        ${kafkaConfig.kafkaConfigMap}
       | bus.trace                                        ${kafkaConfig.cfg.getBoolean("bus.trace")}
       |********************************************************************************************************
       | serviceDescription:
       | ${serviceDescription}
       |********************************************************************************************************
    """.stripMargin)

  val decider: Supervision.Decider = {
    case _ => Supervision.Resume  // Never give up !
  }
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
    .withProperties(kafkaConfig.kafkaConfigMap)
    .withGroupId(service.serviceIdentifier.name)
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
  service.registerServiceStream(getServiceDescriptor _)


  val replyAndCommit = new PartialFunction[(ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent, Any), Future[Done]]{
    def apply(x: (ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent, Any) ) = {
      system.log.debug("******** TypeBus: replyAndCommit")
      system.log.debug(s"listOfImplicitsWriters: ${service.listOfImplicitsWriters}")
      system.log.debug(s"type: ${x._3.getClass.getCanonicalName}")
      if(x._3 != Unit) {
        implicit val timeout = Timeout(4 seconds)
        val retType = x._3.getClass.getCanonicalName
        val publishedEvent = PublishedEvent(
          meta = x._2.meta.copy(
            eventId = UUID.randomUUID.toString,
            eventType = EventType.parse(x._3.getClass.getCanonicalName),
            responseTo = Some(x._2.meta.eventId),
            occurredAt = Instant.now()
          ),
          payload = service.listOfServiceImplicitsWriters.get(EventType.parse(retType)).map{ writer =>
            writer.write(x._3.asInstanceOf[TypeBus])
          }.getOrElse(service.listOfImplicitsWriters(EventType.parse(retType)).write(x._3))
        )
        // RPC clients publish to the "Serivce Name" subscription, where that service then can route message back to RPC client.
        x._2.meta.directReply.filterNot(_.service.name == service.serviceIdentifier.name).foreach{ rpc =>
          publisher.publish( publishedEvent.copy(meta = publishedEvent.meta.copy(eventType = EventType.parse(rpc.service.name) )) )
        }
        publisher.publish(publishedEvent)
      }
      system.log.debug("committableOffset !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      x._1.committableOffset.commitScaladsl()
    }
    def isDefinedAt(x: (ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent, Any) ) = true
  }

  system.log.info(s"\n\nTYPEBUS KAFKA STARTING TO LISTEN ON TOPICS: ${service.serviceIdentifier.name :: (service.listOfFunctions.map(_._1.fqn) ::: service.listOfServiceFunctions.map(_._1.fqn))}")

  Consumer.committableSource(consumerSettings, Subscriptions.topics( (service.serviceIdentifier.name :: service.listOfFunctions.map(_._1.fqn) ::: service.listOfServiceFunctions.map(_._1.fqn)) :_*))
    .mapAsyncUnordered(4) { msg =>
      system.log.info(s"TypeBus: got msg for topic: ${msg.record.topic()}")
      val publish = service.publishedEventReader.read(msg.record.value())
      try {
        publisher.traceEvent(InEventTrace(service.serviceIdentifier, publish), publish.meta)
        consume(publish).map(x => (msg, publish, x))
          //.recover()   // TODO: recover from Future.failure
      }catch{
        case t:Throwable =>
          val error = s"Error consuming event: ${publish.meta.eventType}\n${t.getMessage}"
          publisher.produceErrorReport(t, publish.meta, error)
          throw t
      }
    }
    .mapAsyncUnordered(4)(replyAndCommit)
    .runWith(Sink.ignore)

  publisher.publish(serviceDescription)
}
