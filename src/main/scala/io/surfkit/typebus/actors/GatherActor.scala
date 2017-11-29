package io.surfkit.typebus.actors

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.Cluster
import akka.util.Timeout
import io.surfkit.typebus.Mapper
import io.surfkit.typebus.event._
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.joda.time.DateTime

object GatherActor{
  def props(producer: Producer[Array[Byte], String], mapper: Mapper, timeout: Timeout): Props = Props(classOf[GatherActor], producer, mapper, timeout)

  case class Request(data: m.Model)
}

class GatherActor(producer: Producer[Array[Byte], String], mapper: Mapper, timeout: Timeout) extends Actor with ActorLogging {
  val system = context.system
  import system.dispatcher
  
  val cluster = Cluster(context.system)

  log.debug(s"adding http actor ${self.path.toStringWithoutAddress}")
  var replyTo:ActorRef = null

  val cancel = context.system.scheduler.scheduleOnce(timeout.duration){
    context.stop(self)
  }

  def receive = {
    case msg: GatherActor.Request =>
      replyTo = context.sender()
      try {
        log.debug(s"[GatherActor] publish ${msg.data}")
        producer.send(
          new ProducerRecord[Array[Byte], String](
            msg.data.getClass.getCanonicalName,
            mapper.writeValueAsString(PublishedEvent[m.Model](
              eventId = UUID.randomUUID.toString,
              eventType = msg.data.getClass.getCanonicalName,
              source = s"${cluster.selfAddress}${self.path.toStringWithoutAddress}",
              userIdentifier = None,
              socketId = None,
              correlationId = None,
              occurredAt = new DateTime(),
              publishedAt = new DateTime(),
              payload = msg.data))
          )
        )
      }catch{
        case e:Exception =>
          log.error(e, "Error trying to publish event.")
          cancel.cancel()
          context.stop(self)
      }

    case x:ResponseEvent[_] =>
      log.debug(s"GatherActor posting a reply.... ${x.payload.getClass.getSimpleName}")
      replyTo ! x.payload
      cancel.cancel()
      context.stop(self)

    case _ =>
      log.warning(s"GatherActor ${self.path.toStringWithoutAddress} ...WTF WTF WTF !!!!!!!!")
      cancel.cancel()
      context.stop(self)

  }

  override def postStop() {
    log.debug(s"GatherActor ACTOR STOP !!! ${self.path.toStringWithoutAddress}")
  }

}