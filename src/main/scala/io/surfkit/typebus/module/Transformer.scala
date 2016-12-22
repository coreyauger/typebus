package io.surfkit.typebus.module

import java.util.UUID

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import io.surfkit.typebus.Mapper
import io.surfkit.typebus.event._
import org.apache.kafka.clients.producer.ProducerRecord
import org.joda.time.DateTime

/**
  * Created by suroot on 21/12/16.
  */
trait Transformer extends Module{

  def startTransformer(consumerSettings: ConsumerSettings[Array[Byte], String], producerSettings: ProducerSettings[Array[Byte], String], mapper: Mapper)(implicit system: ActorSystem) = {
    import system.dispatcher
    val decider: Supervision.Decider = {
      case _ => Supervision.Resume  // Never give up !
    }

    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

    val consumerToProducer = new PartialFunction[(ConsumerMessage.CommittableMessage[Array[Byte], String],PublishedEvent[_], m.Model), ProducerMessage.Message[Array[Byte], String, ConsumerMessage.CommittableOffset]]{
      def apply(x: (ConsumerMessage.CommittableMessage[Array[Byte], String],PublishedEvent[_], m.Model) ) = {
        ProducerMessage.Message(new ProducerRecord[Array[Byte], String](
          x._3.getClass.getCanonicalName.replaceAll("\\$", ""),
          mapper.writeValueAsString(PublishedEvent(
            eventId = UUID.randomUUID.toString,
            eventType = x._3.getClass.getCanonicalName.replaceAll("\\$", ""),
            userIdentifier = x._2.userIdentifier,
            source = x._2.source,
            publishedAt = new DateTime(),
            occurredAt = new DateTime(),
            correlationId = x._2.correlationId,
            payload = x._3))
        ), x._1.committableOffset)
      }
      def isDefinedAt(x: (ConsumerMessage.CommittableMessage[Array[Byte], String],PublishedEvent[_], m.Model) ) = true
    }

    Consumer.committableSource(consumerSettings, Subscriptions.topics(listOfTopics:_*))
      .mapAsyncUnordered(1) { msg =>
        val publish = mapper.readValue[PublishedEvent[m.Model]](msg.record.value())
        val event = publish.copy(payload = mapper.readValue[m.Model](mapper.writeValueAsString(publish.payload)) )    // FIXME: we have to write and read again .. grrr !!
        println(s"event: ${event}")
        handleEvent(event.payload).map( x => (msg, event, x) )
      }
      .map(consumerToProducer)
      .runWith(Producer.commitableSink(producerSettings))
  }
}
