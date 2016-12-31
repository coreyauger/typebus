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

/**
  * Created by suroot on 21/12/16.
  */
trait Orchestration extends Module{

  def startOrchestration(consumerSettings: ConsumerSettings[Array[Byte], String], producerSettings: ProducerSettings[Array[Byte], String], mapper: Mapper)(implicit system: ActorSystem) = {
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
          actor ! ResponseEvent(
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

    Consumer.committableSource(consumerSettings, Subscriptions.topics(listOfTopics.head))
      .map { msg =>
        val publish = mapper.readValue[PublishedEvent[m.Model]](msg.record.value())
        val event = publish.copy(payload = mapper.readValue[m.Model](mapper.writeValueAsString(publish.payload)) )    // FIXME: we have to write and read again .. grrr !!
        //println(s"event: ${event}")
        //orchestration(event.payload)
        //handleEvent(event.payload).map( x => (msg, event, x) )
        (msg, event, event.payload)
      }.via(routeAround)
      //.via(Producer.flow(producerSettings))
      //.mapAsyncUnordered(1)(replyAndCommit)
      .runWith(Sink.ignore)
  }

  //def flow[T <: m.Model, U <: m.Model]: Graph[FlowShape[T, U], _]
  val flow: Graph[FlowShape[m.Model, m.Model], _]


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

}
