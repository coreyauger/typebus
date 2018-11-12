package io.surfkit.typebus.actors

import akka.actor.{Actor, ActorLogging, Props}
import io.surfkit.typebus.AvroByteStreams
import io.surfkit.typebus.event._
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}

object ProducerActor{
  def props(producer: Producer[Array[Byte], Array[Byte]]): Props = Props(classOf[ProducerActor], producer)
}

/***
  * ProducerActor - wraps the bus and publishes request to it. (Kafka / Kinesis)
  * Note that we wrap all messages on the bus in a PublishedEvent
  * @param producer - The underlying bus provider (Kafka / Kinesis)
  */
class ProducerActor(producer: Producer[Array[Byte], Array[Byte]]) extends Actor with ActorLogging with AvroByteStreams {

  val publishedEventWriter = new AvroByteStreamWriter[PublishedEvent]

  def receive = {
    case x:PublishedEvent =>
      try {
        log.info(s"[ProducerActor] publish ${x.meta.eventType}")
        producer.send(
          new ProducerRecord[Array[Byte], Array[Byte]](
            x.meta.eventType,
            publishedEventWriter.write(x)
          )
        )
      }catch{
        case e:Exception =>
          log.error(e, "Error trying to publish event.")
      }

    case _ =>
      log.warning(s"ProducerActor ${self.path.toStringWithoutAddress} ...WTF WTF WTF !!!!!!!!")
      context.stop(self)
  }

  override def postStop() {
    log.debug(s"ProducerActor ACTOR STOP !!! ${self.path.toStringWithoutAddress}")
  }

}
