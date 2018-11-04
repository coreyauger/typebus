package io.surfkit.typebus.actors

import java.io.ByteArrayOutputStream

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.sksamuel.avro4s._
import io.surfkit.typebus.event._
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}

object ProducerActor{
  def props(producer: Producer[Array[Byte], Array[Byte]]): Props = Props(classOf[ProducerActor], producer)
}

class ProducerActor(producer: Producer[Array[Byte], Array[Byte]]) extends Actor with ActorLogging {

  def receive = {
    case x:PublishedEvent =>
      try {
        val schema = AvroSchema[PublishedEvent]
        val baos = new ByteArrayOutputStream()
        val output = AvroOutputStream.binary[PublishedEvent].to(baos).build(schema)
        output.write(x)
        output.close()
        println(s"[ProducerActor] publish ${x.meta.eventType}")
        log.info(s"[ProducerActor] publish ${x.meta.eventType}")
        producer.send(
          new ProducerRecord[Array[Byte], Array[Byte]](
            x.meta.eventType,
            baos.toByteArray
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
