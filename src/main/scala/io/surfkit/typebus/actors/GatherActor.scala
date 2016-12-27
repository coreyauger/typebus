package io.surfkit.typebus.actors

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.Cluster
import io.surfkit.typebus.Mapper
import io.surfkit.typebus.event.PublishedEvent
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.joda.time.DateTime

object GatherActor{
  def props(producer: Producer[Array[Byte], String], mapper: Mapper): Props = Props(classOf[GatherActor], producer, mapper)

  case class Request(data: m.Model)
}

class GatherActor(producer: Producer[Array[Byte], String], mapper: Mapper) extends Actor with ActorLogging {
  val cluster = Cluster(context.system)

  log.debug(s"adding http actor ${self.path.toStringWithoutAddress}")
  var replyTo:ActorRef = null

  def receive = {
    case msg: GatherActor.Request =>
      replyTo = context.sender()
      try {
        log.info(s"[GatherActor] publish ${msg.data}")
        producer.send(
          new ProducerRecord[Array[Byte], String](
            msg.data.getClass.getCanonicalName.replaceAll("\\$", ""),
            mapper.writeValueAsString(PublishedEvent[m.Model](
              eventId = UUID.randomUUID.toString,
              eventType = msg.data.getClass.getCanonicalName.replaceAll("\\$", ""),
              source = s"${cluster.selfAddress}${self.path.toStringWithoutAddress}",
              userIdentifier = None,
              correlationId = None,
              occurredAt = new DateTime(),
              publishedAt = new DateTime(),
              payload = msg.data))
          )
        )
      }catch{
        case e:Exception =>
          log.error(e, "Error trying to publish event.")
      }

    case x:PublishedEvent[_] =>
      println("GatherActor posting a reply....")
      replyTo ! x.payload
      context.stop(self)

    case _ =>
      log.warning(s"GatherActor ${self.path.toStringWithoutAddress} ...WTF WTF WTF !!!!!!!!")
      context.stop(self)

  }

  override def postStop() {
    log.debug(s"GatherActor ACTOR STOP !!! ${self.path.toStringWithoutAddress}")
  }

}