package io.surfkit.typebus.module

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

/**
  * Created by suroot on 21/12/16.
  */
trait Orchestration[API] extends Module{

  def perform[T <: m.Model : ClassTag](p: PartialFunction[T, Future[m.Model]]) = op(p)

  def startOrchestration(consumerSettings: ConsumerSettings[Array[Byte], String], /*producerSettings: ProducerSettings[Array[Byte], String],*/ mapper: Mapper)(implicit system: ActorSystem) = {
    import system.dispatcher
    val decider: Supervision.Decider = {
      case _ => Supervision.Resume // Never give up !
    }

    implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

    def reply(reply: Any, x: PublishedEvent[_]) = {
      implicit val timeout = Timeout(4 seconds)
      //println(s"Doing actor selection and send: ${x._3} => ${x._2.source}")
      println(s"Doing actor selection and send: ${x.source}")
      system.actorSelection(x.source).resolveOne().map { actor =>
        actor ! ResponseEvent(
          eventId = UUID.randomUUID.toString,
          eventType = reply.getClass.getCanonicalName.replaceAll("\\$", ""),
          userIdentifier = x.userIdentifier,
          source = x.source,
          socketId = x.socketId,
          publishedAt = new DateTime(),
          occurredAt = new DateTime(),
          correlationId = x.correlationId,
          payload = reply)
      }
    }

    val replyAndCommit = new PartialFunction[(ConsumerMessage.CommittableMessage[Array[Byte], String], PublishedEvent[_], Any), Future[Done]] {
      def apply(x: (ConsumerMessage.CommittableMessage[Array[Byte], String], PublishedEvent[_], Any)) = {
        println("replyAndCommit")
        reply(x._3, x._2).flatMap{ _ =>
          x._1.committableOffset.commitScaladsl()
        }
      }

      def isDefinedAt(x: (ConsumerMessage.CommittableMessage[Array[Byte], String], PublishedEvent[_], Any)) = true
    }

    Consumer.committableSource(consumerSettings, Subscriptions.topics(listOfTopics: _*))
      .mapAsyncUnordered(4) { msg =>
        val publish = mapper.readValue[PublishedEvent[m.Model]](msg.record.value())
        val event = publish.copy(payload = mapper.readValue[m.Model](mapper.writeValueAsString(publish.payload))) // FIXME: we have to write and read again .. grrr !!
        handleOrchestrate(event).map(x => (msg, event, x))
      }
      .mapAsyncUnordered(4)(replyAndCommit)
      .runWith(Sink.ignore)
  }

  //def flow[T <: m.Model, U <: m.Model]: Graph[FlowShape[T, U], _]
  /*val flow: Graph[FlowShape[m.Model, m.Model], _]


  val routeAround =
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      // prepare graph elements
      val broadcast = b.add(Broadcast[(ConsumerMessage.CommittableMessage[Array[Byte], String],PublishedEvent[_], m.Model)](2))
      val zip = b.add(Zip[(ConsumerMessage.CommittableMessage[Array[Byte], String],PublishedEvent[_]), m.Model]())

      // connect the graph
      broadcast.out(0).map(x => (x._1, x._2)) ~> zip.in0
      broadcast.out(1).map(_._3).via(flow) ~>  zip.in1

      // expose ports
      FlowShape(broadcast.in, zip.out)
    })
*/
}
