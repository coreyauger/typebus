package io.surfkit.typebus.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.surfkit.typebus.Mapper
import io.surfkit.typebus.event.PublishedEvent
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}

object ProducerActor{
  def props(producer: Producer[Array[Byte], String], mapper: Mapper): Props = Props(classOf[GatherActor], producer, mapper)
}

class ProducerActor(producer: Producer[Array[Byte], String], mapper: Mapper) extends Actor with ActorLogging {
  //val cluster = Cluster(context.system)

  log.debug(s"adding http actor ${self.path.toStringWithoutAddress}")
  var replyTo:ActorRef = null

  def receive = {
    case x:PublishedEvent[_] =>
      try {
        println(s"[ProducerActor] publish ${x.payload.getClass}")
        log.info(s"[ProducerActor] publish ${x.payload.getClass}")
        producer.send(
          new ProducerRecord[Array[Byte], String](
            x.payload.getClass.getCanonicalName.replaceAll("\\$", ""),
            mapper.writeValueAsString(x)
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