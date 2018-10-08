package io.surfkit.typebus.module
/*
import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.Timeout
import io.surfkit.typebus.Mapper
import io.surfkit.typebus.event._

import scala.concurrent.duration._
import org.apache.kafka.clients.producer.ProducerRecord
import org.joda.time.DateTime
import akka.stream._
import akka.stream.scaladsl._

import scala.concurrent.Future
import scala.reflect.ClassTag
import com.sksamuel.avro4s._
/**
  * Created by suroot on 21/12/16.
  */
trait Orchestration extends Module{

  def perform[T <: m.Model : ClassTag](p: PartialFunction[PublishedEvent[T], Future[m.Model]]) = orchestrate(p)

  def reply(reply: Any, x: PublishedEvent[_])(implicit system: ActorSystem) = {
    import system.dispatcher
    implicit val timeout = Timeout(4 seconds)
    //println(s"Doing actor selection and send: ${x._3} => ${x._2.source}")
    println(s"Doing actor selection and send: ${x.source}")
    system.actorSelection(x.source).resolveOne().map { actor =>
      actor ! ResponseEvent(
        eventId = UUID.randomUUID.toString,
        eventType = reply.getClass.getCanonicalName,
        userIdentifier = x.userIdentifier,
        source = x.source,
        socketId = x.socketId,
        publishedAt = new DateTime(),
        occurredAt = new DateTime(),
        correlationId = x.correlationId,
        payload = reply)
    }
  }

  def startOrchestration(consumerSettings: ConsumerSettings[Array[Byte], Array[Byte]])(implicit system: ActorSystem) = {
    import system.dispatcher
    val decider: Supervision.Decider = {
      case _ => Supervision.Resume // Never give up !
    }

    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
    val replyAndCommit = new PartialFunction[(ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]], PublishedEvent[_], Any), Future[Done]] {
      def apply(x: (ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]], PublishedEvent[_], Any)) = {
        println("Orchestration replyAndCommit")
        reply(x._3, x._2).flatMap{ _ =>
          x._1.committableOffset.commitScaladsl()
        }
      }
      def isDefinedAt(x: (ConsumerMessage.CommittableMessage[Array[Byte], Array[Byte]], PublishedEvent[_], Any)) = true
    }

    println("=================== Orchestration")
    listOfTopics.foreach(println)
    Consumer.committableSource(consumerSettings, Subscriptions.topics(listOfTopics: _*))
      .mapAsyncUnordered(4) { msg =>
        val publish = io.surfkit.typebus.Mapper.fromByteStream(msg.record.value())
        println(s"Orchestration event: ${publish}")
        handleOrchestrate(publish).map(x => (msg, publish, x)).recover{
          case t: Throwable =>
            println(s"ERROR handling event: ${publish.payload.getClass.getName}")
            println(s"ERROR payload: ${publish.payload}")
            t.printStackTrace()
            throw t
        }
      }
      .mapAsyncUnordered(4)(replyAndCommit)
      .runWith(Sink.ignore)
  }
}
*/