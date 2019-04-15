package io.surfkit.typebus.bus.kafka

import java.io.{PrintWriter, StringWriter}

import akka.Done
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, OverflowStrategy, Supervision}
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
      event.meta.key match{
        case Some(partitionKey) => producer.send(new ProducerRecord[Array[Byte], Array[Byte]](event.meta.eventType.fqn, partitionKey.map(_.toByte).toArray, publishedEventWriter.write(event)))
        case _ => producer.send(new ProducerRecord[Array[Byte], Array[Byte]](event.meta.eventType.fqn, publishedEventWriter.write(event)))
      }
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
  val backChannelSettings = ConsumerSettings(system, new ByteArrayDeserializer, new ByteArrayDeserializer)
    .withProperties(kafkaConfig.kafkaConfigMap)
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
      val retType = x._3.getClass.getCanonicalName
      system.log.info(s"replyAndCommit for type: ${retType}")
      val eventId = UUID.randomUUID.toString
      if(x._3 != Unit) {
        implicit val timeout = Timeout(4 seconds)

        val sb = service.streamBuilderMap(EventType.parse(retType))
        val partitionKey = sb.partitionKey.flatMap(_ => sb.untyped(x._3) )   // FIXME: this untyped bit sux
        val publishedEvent = PublishedEvent(
          meta = x._2.meta.copy(
            eventId = eventId,
            eventType = EventType.parse(x._3.getClass.getCanonicalName),
            responseTo = Some(x._2.meta.eventId),
            key = partitionKey,
            occurredAt = Instant.now()
          ),
          payload = service.listOfServiceImplicitsWriters.get(EventType.parse(retType)).map{ writer =>
            writer.write(x._3.asInstanceOf[TypeBus])
          }.getOrElse(service.listOfImplicitsWriters(EventType.parse(retType)).write(x._3))
        )
        // RPC clients publish to the "Serivice Name" subscription, where that service then can route message back to RPC client.
        x._2.meta.directReply.filterNot(_.service.name == service.serviceIdentifier.name).foreach{ rpc =>
          publisher.publish( publishedEvent.copy(meta = publishedEvent.meta.copy(eventType = EventType.parse(rpc.service.name), key = partitionKey )) )
        }
        publisher.publish(publishedEvent)
      }
      system.log.debug(s"typebus kafka commit offset for event: ${retType} with eventId: ${}")
      x._1.committableOffset.commitScaladsl()
    }
    def isDefinedAt(x: (ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent, Any) ) = true
  }

  system.log.info(s"\n\nTYPEBUS KAFKA STARTING TO LISTEN ON TOPICS: ${(service.serviceIdentifier.name :: (service.listOfFunctions.map(_._1.fqn) ::: service.listOfServiceFunctions.map(_._1.fqn))).mkString("\n")}")

  val bufferSize = 16 // TODO: make this configurable
  val parrallelism = 4 // TODO: make config
  val retryQueueSource = Source.queue[(ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent)](bufferSize, OverflowStrategy.dropHead)
  val retryQueue = retryQueueSource.toMat(Sink.ignore)(Keep.left).run()

  def startConsumerGraph(settings: ConsumerSettings[Array[Byte], Array[Byte]], topics: String*) =
    Consumer.committableSource(settings, Subscriptions.topics(topics:_*))
      .map{ msg =>
        system.log.info(s"TypeBus: got msg for topic: ${msg.record.topic()}")
        (msg, service.publishedEventReader.read(msg.record.value()))
      }
      .merge(retryQueueSource)
      .mapAsyncUnordered(parrallelism) { case (msg, publish) =>
        try {
          publisher.traceEvent(InEventTrace(service.serviceIdentifier, publish), publish.meta)
          consume(publish).map(x => (msg, publish, x))
            .recover{ case t: Throwable => (msg, publish, Recoverable(t)) }
        }catch{ case t:Throwable => Future.successful(msg, publish, Recoverable(t)) }
      }.statefulMapConcat( () => {
        val retryState = scala.collection.mutable.HashMap.empty[String, (Int, RetryPolicy)]
        elm => {
          elm match {
            case (msg, publish, Recoverable(t)) =>
              publisher.produceErrorReport(t, publish.meta, s"Error consuming event: ${publish.meta.eventType}\n${t.getMessage}")
              val (attempt, retryPolicy): (Int,RetryPolicy) = retryState.get(publish.meta.eventId).getOrElse {
                val p = service.streamBuilderMap(publish.meta.eventType).retry.map { policy =>
                  if (policy.isDefinedAt(t)) policy(t)
                  else RetryPolicy.Fail
                }.getOrElse(RetryPolicy.Fail)
                retryState += publish.meta.eventId -> (1, p)
                (1, p)
              }
              retryPolicy match{
                case RetryPolicy(numRetry, delay, backoff) if attempt >= numRetry =>
                  msg.committableOffset.commitScaladsl()  // is this right if we have this here.. IE the Future[Done] is not propagated down stream?
                  retryState -= publish.meta.eventId
                  Nil
                case RetryPolicy(_, delay, backoff) =>
                  val timeout = backoff match{
                    case RetryBackoff.None => delay
                    case RetryBackoff.Linear => attempt * delay
                    case RetryBackoff.Exponential => (attempt*attempt) * delay
                  }
                  system.scheduler.scheduleOnce(timeout){
                    retryState += publish.meta.eventId -> (attempt+1, retryPolicy)
                    retryQueue.offer((msg, publish)) // retry injecting upstream
                  }
                  Nil
              }
            case (_, publish, _) =>
              retryState -= publish.meta.eventId
              elm :: Nil
          }
        }
      })
      .mapAsyncUnordered(parrallelism)(replyAndCommit)
      .runWith(Sink.ignore)

  startConsumerGraph(consumerSettings, (service.serviceIdentifier.name :: service.listOfFunctions.map(_._1.fqn) ::: service.listOfServiceFunctions.map(_._1.fqn)) :_*)
  startConsumerGraph(backChannelSettings, service.listOfServiceFunctions.map(_._1.fqn): _*)

  publisher.publish(serviceDescription)
}
