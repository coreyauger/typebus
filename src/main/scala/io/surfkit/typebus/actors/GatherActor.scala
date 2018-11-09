package io.surfkit.typebus.actors

import java.util.UUID

import akka.actor._
import akka.cluster.Cluster
import akka.util.Timeout
import io.surfkit.typebus.{AvroByteStreams, ByteStreamWriter}
import io.surfkit.typebus.Mapper._
import io.surfkit.typebus.event._
import org.apache.kafka.clients.producer.{Producer, ProducerRecord}
import org.joda.time.DateTime

import scala.reflect.ClassTag

object GatherActor{
  def props[T](producer: Producer[Array[Byte], Array[Byte]], timeout: Timeout)(implicit writer: ByteStreamWriter[PublishedEvent]): Props = Props(classOf[GatherActor[T]], producer, timeout, writer)

  case class Request[T](data: T)
}

class GatherActor[T : ClassTag](producer: Producer[Array[Byte], Array[Byte]], timeout: Timeout, writer: ByteStreamWriter[T]) extends Actor with ActorLogging with AvroByteStreams{
  val system = context.system
  import system.dispatcher
  
  val cluster = Cluster(context.system)
  val correlationId = UUID.randomUUID().toString

  log.debug(s"adding http actor ${self.path.toStringWithoutAddress}")
  def clusterPath = s"${cluster.selfAddress}${self.path.toStringWithoutAddress}"
  var replyTo:ActorRef = null

  val cancel = context.system.scheduler.scheduleOnce(timeout.duration){
    log.warning("GatherActor timeout ... shutting down!")
    context.stop(self)
  }

  val publishedEventWriter = new AvroByteStreamWriter[PublishedEvent]

  def receive = {
    case msg: GatherActor.Request[T] =>
      replyTo = context.sender()
      try {
        log.debug(s"[GatherActor] publish ${msg.data}")
        val publishedEvent = PublishedEvent(
          meta = EventMeta(
            eventId = UUID.randomUUID().toString,
            eventType = msg.data.getClass.getCanonicalName,
            source = clusterPath,
            directReply = Some(clusterPath),
            correlationId = Some(correlationId)
          ),
          payload = writer.write(msg.data)
        )
        producer.send(
          new ProducerRecord[Array[Byte], Array[Byte]](
            publishedEvent.meta.eventType,
            publishedEventWriter.write(publishedEvent)
          )
        )
      }catch{
        case e:Exception =>
          log.error(e, "Error trying to publish event.")
          cancel.cancel()
          context.stop(self)
      }

    case x:PublishedEvent =>
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
