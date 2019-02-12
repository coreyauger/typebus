package io.surfkit.typebus.actors

import java.io.{PrintWriter, StringWriter}
import java.time.Instant
import java.util.UUID

import akka.actor._
import akka.cluster.Cluster
import akka.util.Timeout
import io.surfkit.typebus.bus.Publisher
import io.surfkit.typebus.{AvroByteStreams, ByteStreamReader, ByteStreamWriter}
import io.surfkit.typebus.event._

import scala.reflect.ClassTag

object GatherActor{
  //def props[T, U](producer: Publisher[T], timeout: Timeout, writer: ByteStreamWriter[T], reader: ByteStreamReader[U]): Props = Props(classOf[GatherActor[T, U]], producer, timeout, writer)

  /***
    * Request - wrapper for the service call request type T
    * @param data - The service call of type T
    * @tparam T - The service call type
    */
  case class Request[T](data: T)
}

/***
  * GatherActor - An actor per request RPC actor
  * @param producer - the bus producer to push a request onto the BUS
  * @param timeout - a passed in configurable timeout for the request
  * @param writer - ByteStreamWriter (defaults to avro)
  * @param reader - ByteStreamReader (defaults to avro)
  * @param ev$1 - evidence parameter
  * @tparam T - The IN type for the service call
  * @tparam U - The OUT type in the service called. Wrapped as Future[U]
  */
class GatherActor[T : ClassTag, U : ClassTag](serviceIdentifier: ServiceIdentifier, producer: Publisher, timeout: Timeout, writer: ByteStreamWriter[T], reader: ByteStreamReader[U]) extends Actor with ActorLogging with AvroByteStreams{
  implicit val system = context.system
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

  def receive = {
    case msg: GatherActor.Request[T] =>
      replyTo = context.sender()
      val meta =  EventMeta(
        eventId = UUID.randomUUID().toString,
        eventType = EventType.parse(msg.data.getClass.getCanonicalName),
        source = clusterPath,
        directReply = Some(RpcClient(clusterPath, serviceIdentifier)),
        correlationId = Some(correlationId)
      )
      try {
        log.debug(s"[GatherActor] publish ${msg.data}")
        val outEvent = PublishedEvent(
          meta = meta,
          payload = writer.write(msg.data)
        )
        producer.publish(outEvent)
        producer.traceEvent(serviceIdentifier)( { serviceId: ServiceIdentifier =>
          OutEventTrace(serviceId.service, serviceId.serviceId, outEvent)
        }, outEvent.meta)
      }catch{
        case t:Exception =>
          producer.produceErrorReport(serviceIdentifier)(t,meta)
          cancel.cancel()
          context.stop(self)
      }

    case x:PublishedEvent =>
      log.debug(s"GatherActor posting a reply.... ${x.payload.getClass.getSimpleName}")
      try{
        val responsePayload = reader.read(x.payload)
        replyTo ! responsePayload
        producer.traceEvent(serviceIdentifier)( { serviceId: ServiceIdentifier =>
          InEventTrace(serviceId.service, serviceId.serviceId, x)
        }, x.meta)
      }catch{
        case t: Throwable =>
          producer.produceErrorReport(serviceIdentifier)(t, x.meta)
      }finally {
        cancel.cancel()
        context.stop(self)
      }

    case _ =>
      log.warning(s"GatherActor ${self.path.toStringWithoutAddress} ...WTF WTF WTF !!!!!!!!")
      cancel.cancel()
      context.stop(self)

  }

  override def postStop() {
    log.debug(s"GatherActor ACTOR STOP !!! ${self.path.toStringWithoutAddress}")
  }

}
