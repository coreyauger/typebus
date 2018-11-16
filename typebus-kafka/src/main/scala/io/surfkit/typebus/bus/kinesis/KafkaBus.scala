package io.surfkit.typebus.bus.kinesis

import java.util.UUID

import akka.Done
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import io.surfkit.typebus.actors.ProducerActor
import io.surfkit.typebus.bus.Bus
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.concurrent.duration._
import scala.concurrent.Future

trait KafkaBus[UserBaseType] extends Bus[UserBaseType]{
  service: Service[UserBaseType] =>

  val cfg = ConfigFactory.load
  val kafka = cfg.getString("bus.kafka")
  import collection.JavaConversions._
  val producer = new KafkaProducer[Array[Byte], Array[Byte]](Map(
    "bootstrap.servers" -> kafka,
    "key.serializer" ->  "org.apache.kafka.common.serialization.ByteArraySerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer"
  ))

  val publishedEventWriter = new AvroByteStreamWriter[PublishedEvent]
  def publish(event: PublishedEvent): Unit = {
    try {
      println(s"publish ${event.meta.eventType}")
      producer.send(
        new ProducerRecord[Array[Byte], Array[Byte]](
          event.meta.eventType,
          publishedEventWriter.write(event)
        )
      )
    }catch{
      case e:Exception =>
        println("Error trying to publish event.")
        e.printStackTrace()
    }
  }

  def busActor(implicit system: ActorSystem): ActorRef =
    system.actorOf(Props(new ProducerActor(this)))

  def startTypeBus(serviceName: String)(implicit system: ActorSystem): Unit = {
    import system.dispatcher
    val decider: Supervision.Decider = {
      case _ => Supervision.Resume  // Never give up !
    }
    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

    val serviceDescription = makeServiceDescriptor(serviceName)
    implicit val serviceDescriptorWriter = new AvroByteStreamWriter[ServiceDescriptor]
    implicit val getServiceDescriptorReader = new AvroByteStreamReader[GetServiceDescriptor]

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
      println("**************** getServiceDescriptor")
      println(serviceDescription)
      Future.successful(serviceDescription)
    }
    registerServiceStream(getServiceDescriptor _)


    val replyAndCommit = new PartialFunction[(ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent, Any), Future[Done]]{
      def apply(x: (ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent, Any) ) = {
        println("******** TypeBus: replyAndCommit")
        system.log.debug(s"listOfImplicitsWriters: ${listOfImplicitsWriters}")
        system.log.debug(s"type: ${x._3.getClass.getCanonicalName}")
        if(x._3 != Unit) {
          implicit val timeout = Timeout(4 seconds)
          val retType = x._3.getClass.getCanonicalName
          val publishedEvent = PublishedEvent(
            meta = x._2.meta.copy(
              eventId = UUID.randomUUID.toString,
              eventType = x._3.getClass.getCanonicalName,
              responseTo = Some(x._2.meta.eventId)
            ),
            payload = listOfServiceImplicitsWriters.get(retType).map{ writer =>
              writer.write(x._3.asInstanceOf[TypeBus])
            }.getOrElse(listOfImplicitsWriters(retType).write(x._3.asInstanceOf[UserBaseType]))
          )
          x._2.meta.directReply.foreach( system.actorSelection(_).resolveOne().map( actor => actor ! publishedEvent ) )
          publish(publishedEvent)
        }
        println("committableOffset !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        x._1.committableOffset.commitScaladsl()
      }
      def isDefinedAt(x: (ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent, Any) ) = true
    }

    system.log.debug(s"STARTING TO LISTEN ON TOPICS:\n ${listOfFunctions.map(_._1)}")

    Consumer.committableSource(consumerSettings, Subscriptions.topics( (listOfFunctions.map(_._1) ::: listOfServiceFunctions.map(_._1)) :_*))
      .mapAsyncUnordered(4) { msg =>
        system.log.debug(s"TypeBus: got msg for topic: ${msg.record.topic()}")
        try {
          val reader = listOfServiceImplicitsReaders.get(msg.record.topic()).getOrElse(listOfImplicitsReaders(msg.record.topic()))
          val publish = publishedEventReader.read(msg.record.value())
          system.log.debug(s"TypeBus: got publish: ${publish}")
          system.log.debug(s"TypeBus: reader: ${reader}")
          system.log.debug(s"publish.payload.size: ${publish.payload.size}")
          val payload = reader.read(publish.payload)
          system.log.debug(s"TypeBus: got payload: ${payload}")
          if(handleEventWithMetaUnit.isDefinedAt( (payload, publish.meta) ) )
            handleEventWithMetaUnit( (payload, publish.meta) ).map(x => (msg, publish, x))
          else if(handleEventWithMeta.isDefinedAt( (payload, publish.meta) ) )
            handleEventWithMeta( (payload, publish.meta)  ).map(x => (msg, publish, x))
          else if(handleServiceEventWithMeta.isDefinedAt( (payload, publish.meta) ) )
            handleServiceEventWithMeta( (payload, publish.meta)  ).map(x => (msg, publish, x))
          else
            handleEvent(payload).map(x => (msg, publish, x))
        }catch{
          case t:Throwable =>
            t.printStackTrace()
            throw t
        }
      }
      .mapAsyncUnordered(4)(replyAndCommit)
      .runWith(Sink.ignore)
  }
}
