package io.surfkit.telemetry.actors

import akka.actor._
import java.util.UUID
import akka.cluster.sharding.ClusterSharding
import akka.util.Timeout
import scala.concurrent.duration._
import io.surfkit.telemetry.cluster.UserActor
import io.surfkit.telemetry.data.BaseType
import io.surfkit.typebus.bus.kafka.KafkaBus
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import java.time.Instant
object CoreActor {

  sealed trait Socket
  case class Connect(uuid: String, socketId:UUID, subscriber: ActorRef, token: String) extends Socket
  case class Disconnect(uuid: String, socketId:UUID) extends Socket
  case class ReceivedMessage(uuid: String, socketId:UUID, data: SocketEvent) extends Socket {
    def toPublishedEvent:PublishedEvent = PublishedEvent(
      meta = data.meta.copy(
        eventId = UUID.randomUUID().toString,
        occurredAt = Instant.now()
      ),
      payload = data.payload)

  }
}

class CoreActor extends Service[BaseType]("core-actor") with Actor with KafkaBus[BaseType] with ActorLogging{
  implicit val system = context.system
  var subscriberToUserId = Map.empty[ActorRef, String]
  var socketIdToSubscriber = Map.empty[UUID, ActorRef]  // HACK: way to handle Disconnect faster then "actor.watch"
  import CoreActor._

  val bus = busActor

  implicit val timeout = Timeout(10 seconds)

  lazy val userRegion = ClusterSharding(system).shardRegion("UserActor")

  override def receive: Receive = {
    case Connect(userid, socketId, subscriber, token) =>
      context.watch(subscriber)
      DispatchActor.adminUserIds += userid   // HACK: FIXME:
      subscriberToUserId += subscriber -> userid
      socketIdToSubscriber += socketId -> subscriber
      userRegion ! UserActor.ShardMessage(UUID.fromString(userid), UserActor.Connect(socketId, subscriber))
      log.info(s"$userid joined!")

    case msg: ReceivedMessage =>
      val produce = msg.toPublishedEvent
      println(s"TO PUBLISH EVENT: ${produce}")
      // CA - filter out HearBeat (Hb) events at this level.
      if( !msg.data.meta.eventType.endsWith(Hb.getClass.getSimpleName.replaceAllLiterally("$","") ) )
        userRegion ! UserActor.ShardMessage(UUID.fromString(msg.uuid), produce)

    case Disconnect(uuid, socketId) =>
      log.info(s"user($uuid) socketId(${socketId}) left!")
      for {
        s <- socketIdToSubscriber.get(socketId)
      } yield{
        //ClusterStats.inc(ClusterStats.StatCategory.Socket, m.Socket.Closed("").getClass.getName)    // record connect to stats
        log.info(s"Sending disconnect to user actor($uuid)")
        //userRegion ! UserActor.ShardMessage(UUID.fromString(uuid), UserActor.DisConnect(socketId, s))
        subscriberToUserId -= s
        socketIdToSubscriber -= socketId
      }

    case Terminated(sub) =>
      log.info("Terminated")
      subscriberToUserId.get(sub).foreach{ uuid =>
        socketIdToSubscriber.find{ case (id, s) => s == sub }.foreach{ case (socketId, ref) =>
          log.info(s"Terminated :: Sending disconnect to user actor($uuid)")
          userRegion ! UserActor.ShardMessage(UUID.fromString(uuid), UserActor.DisConnect(socketId, sub))
        }
        subscriberToUserId -= sub
        socketIdToSubscriber = socketIdToSubscriber.filterNot(_._2 == sub)
      }
      subscriberToUserId -= sub // clean up dead subscribers
  }
}
