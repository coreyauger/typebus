package io.surfkit.typebus.module

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import io.surfkit.typebus.{ByteStreamReader, Mapper}

import scala.concurrent.Future
import scala.concurrent.duration._
import io.surfkit.typebus.event._
import org.joda.time.DateTime

import scala.reflect.ClassTag

/**
  * Created by suroot on 21/12/16.
  */
trait Service extends Module{

  def perform[T : ClassTag, U <: AnyVal](p: PartialFunction[T, Future[U]])(implicit reader: ByteStreamReader[PublishedEvent[U]] ) =
    op(p)

  def startService(consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]])(implicit system: ActorSystem) = {
    import system.dispatcher
    val decider: Supervision.Decider = {
      case _ => Supervision.Resume  // Never give up !
    }

    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

    val replyAndCommit = new PartialFunction[(ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent[_], Any), Future[Done]]{
      def apply(x: (ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent[_], Any) ) = {
        //println("replyAndCommit")
        implicit val timeout = Timeout(4 seconds)
        //println(s"Doing actor selection and send: ${x._3} => ${x._2.source}")
        //println(s"Doing actor selection and send: ${x._2.source}")
        system.actorSelection(x._2.source).resolveOne().flatMap { actor =>
          actor ! ResponseEvent(
            eventId = UUID.randomUUID.toString,
            eventType = x._3.getClass.getCanonicalName,
            userIdentifier = x._2.userIdentifier,
            source = x._2.source,
            socketId = x._2.socketId,
            publishedAt = new DateTime(),
            occurredAt = new DateTime(),
            correlationId = x._2.correlationId,
            payload = x._3)
          x._1.committableOffset.commitScaladsl()
        }
      }
      def isDefinedAt(x: (ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]],PublishedEvent[_], Any) ) = true
    }

    Consumer.committableSource(consumerSettings, Subscriptions.topics(listOfTopics:_*))
      .mapAsyncUnordered(4) { msg =>
        val reader = listOfImplicits(msg.record.topic())
        val publish = reader.read(msg.record.value())
        val event = publish.payload
        //val event = publish.copy(payload = mapper.readValue[m.Model](mapper.writeValue(publish.payload)) )
        handleEvent(event).map( x => (msg, publish, x) ).recover{
          case t: Throwable =>
            println(s"ERROR handling event: ${event.getClass.getName}")
            println(s"ERROR payload: ${event}")
            t.printStackTrace()
            throw t
        }
      }
      .mapAsyncUnordered(4)(replyAndCommit)
      .runWith(Sink.ignore)
  }
}
