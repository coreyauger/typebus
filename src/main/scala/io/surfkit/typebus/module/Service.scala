package io.surfkit.typebus.module

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import io.surfkit.typebus.Mapper

import scala.concurrent.Future
import scala.concurrent.duration._
import io.surfkit.typebus.event._
import org.joda.time.DateTime

/**
  * Created by suroot on 21/12/16.
  */
trait Service extends Module{

  def startService(consumerSettings: ConsumerSettings[Array[Byte], String], mapper: Mapper)(implicit system: ActorSystem) = {
    import system.dispatcher
    val decider: Supervision.Decider = {
      case _ => Supervision.Resume  // Never give up !
    }

    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

    val replyAndCommit = new PartialFunction[(ConsumerMessage.CommittableMessage[Array[Byte], String],PublishedEvent[_], m.Model), Future[Done]]{
      def apply(x: (ConsumerMessage.CommittableMessage[Array[Byte], String],PublishedEvent[_], m.Model) ) = {
        implicit val timeout = Timeout(4 seconds)
        println(s"Doing actor selection and send: ${x._3} => ${x._2.source}")
        system.actorSelection(x._2.source).resolveOne().flatMap { actor =>
          actor ! PublishedEvent(
            eventId = UUID.randomUUID.toString,
            eventType = x._3.getClass.getCanonicalName.replaceAll("\\$", ""),
            userIdentifier = x._2.userIdentifier,
            source = x._2.source,
            publishedAt = new DateTime(),
            occurredAt = new DateTime(),
            correlationId = x._2.correlationId,
            payload = x._3)
          x._1.committableOffset.commitScaladsl()
        }
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
      .mapAsyncUnordered(1)(replyAndCommit)
      .runWith(Sink.ignore)
  }
}
