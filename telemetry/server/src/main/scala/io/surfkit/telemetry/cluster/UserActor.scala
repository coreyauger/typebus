package io.surfkit.telemetry.cluster

import java.util.UUID

import scala.concurrent.duration._
import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props, ReceiveTimeout, Terminated}
import akka.cluster.Cluster
import akka.cluster.sharding.ShardRegion
import akka.persistence.{PersistentActor, SnapshotOffer}
import io.surfkit.typebus.event.{PublishedEvent, SocketEvent}

import scala.concurrent.Future


object UserActor extends io.surfkit.typebus.cluster.Actor.ActorSharding{
  def props(bus: ActorRef): Props = Props(classOf[UserActor], bus)

  case class Connect(socketId: UUID, subscriber: ActorRef)
  case class DisConnect(socketId: UUID, subscriber: ActorRef)

  val idExtractor: ShardRegion.ExtractEntityId = {
    case cmd: Command => (cmd.uuid.toString, cmd)
  }
  val shardResolver: ShardRegion.ExtractShardId = {
    case cmd: Command => (math.abs(cmd.uuid.toString.hashCode) % numberOfShards).toString
  }
}


class UserActor(bus: ActorRef) extends Actor with ActorLogging with Serializable {
  implicit val system = context.system
  import system.dispatcher
  import ShardRegion.Passivate
  import io.surfkit.typebus.cluster.Actor.RichPartial

  var userId: UUID = UUID.fromString(self.path.name)
  var sources = Map.empty[UUID, ActorRef]
  var numConnects = 1
  var numDisconnects = 0
  var numMessages = 0
  lazy val cluster = Cluster(context.system)

  def clusterPath = s"${cluster.selfAddress}${self.path.toStringWithoutAddress}"

  // PersistentActor
  override def receive: Receive = active

  // actor should passivate after 20 min of no messages
  // https://doc.akka.io/docs/akka/2.5/java/cluster-sharding.html#passivation
  context.setReceiveTimeout(20 minutes)


  def extractMessage(any: Any): Any = any match {
    case UserActor.ShardMessage(_, msg) => msg
    case _ => any
  }

  def active: Receive = process composePartial extractMessage

  def process: Receive = {
    // tell parent actor to send us a poisinpill
    // https://github.com/boldradius/akka-dddd-template/blob/master/src/main/scala/com/boldradius/cqrs/Passivation.scala
    case ReceiveTimeout =>
      log.info(s" UserActor[${userId}]: ReceiveTimeout: passivating. ")
      context.parent ! Passivate(stopMessage = PoisonPill)
    case PoisonPill => context.stop(self)
    case Terminated(source) => socketDisconnect(source)
    case UserActor.DisConnect(socketId, source) => socketDisconnect(source)
    case UserActor.Connect(socketId, source) =>
      numConnects = numConnects + 1
      sources += socketId -> source
      context watch source
      log.info(s"UserActor => Connect.  There are ${sources.size} websocket connections to this UserActor.")

    case x: PublishedEvent =>
      println(s"USER got publishEvent: ${x.meta.eventType}")
      x.meta.responseTo match{
        case Some(eventId) => // CA - you could look up event id to find "which" http hook to send down. Since this is WS we don't care.
            sendDownSocket(SocketEvent(meta=x.meta, payload = x.payload), x.meta.socketId)
        case None =>
          bus ! x.copy(meta = x.meta.copy(source = clusterPath, extra = Map("user" -> userId.toString) ) )
      }
    case x: SocketEvent =>
      log.info(s"SENDING DOWN SOCKET: ${x}")
      sendDownSocket(x, x.meta.socketId)

    case x =>
      log.warning(s"UserActor handling unknown type: ${x.getClass.getName}")
  }

  def sendDownSocket(msg: SocketEvent, socketId: Option[String] = None) = socketId match {
    case Some(sid) =>
      sources.get(UUID.fromString(sid)).map(_ ! msg)
    case None => // CA - broadcast ...
      sources.values.foreach(_ ! msg)
  }


  def socketDisconnect(source: ActorRef) = {
    numDisconnects = numDisconnects + 1
    sources.find(_._2 == source).foreach{ case (id, source) => sources -= id }
    log.info(s"UserActor => DisConnect. There are ${sources.size} websocket connections to this UserActor.")
  }

}