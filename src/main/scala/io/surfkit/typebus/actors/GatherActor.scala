package io.surfkit.typebus.actors

import java.util.UUID

import akka.actor.{Actor, ActorLogging}
import akka.kafka.ProducerSettings
import akka.cluster.Cluster
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import io.surfkit.typebus.Mapper
import io.surfkit.typebus.event.PublishedEvent
import org.apache.kafka.clients.producer.ProducerRecord
import org.joda.time.DateTime

/**
  * Created by suroot on 21/12/16.
  */
class GatherActor(param: m.Model, producerSettings: ProducerSettings[Array[Byte], String], mapper: Mapper) extends Actor with ActorLogging{
/*

  val done = Source(1 to 100)
    .map { n =>
      // val partition = math.abs(n) % 2
      val partition = 0
      ProducerMessage.Message(new ProducerRecord[Array[Byte], String](
        "topic1", partition, null, n.toString
      ), n)
    }
    .via(Producer.flow(producerSettings))
    .map { result =>
      val record = result.message.record
      println(s"${record.topic}/${record.partition} ${result.offset}: ${record.value}" +
        s"(${result.message.passThrough})")
      result
    }
    .runWith(Sink.ignore)

  implicit val materializer = ActorMaterializer()
  val cluster = Cluster(context.system)

  val done = Source(params)
    .map { model =>
      new ProducerRecord[Array[Byte], String](
        model.getClass.getCanonicalName.replaceAll("\\$", ""),
        mapper.writeValueAsString(PublishedEvent(
          eventId = UUID.randomUUID.toString,
          eventType = model.getClass.getCanonicalName.replaceAll("\\$", ""),
          userIdentifier = None,
          source = s"${cluster.selfAddress}${self.path.toStringWithoutAddress}",
          publishedAt = new DateTime(),
          occurredAt = new DateTime(),
          correlationId = Some(UUID.randomUUID.toString),
          payload = model))
      )
    }
    .runWith(Producer.plainSink(producerSettings))

*/

  def receive = {
    case response => // send back...

  }
}
