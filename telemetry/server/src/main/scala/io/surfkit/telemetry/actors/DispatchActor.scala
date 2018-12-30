package io.surfkit.telemetry.actors


import java.util.UUID

import io.surfkit.typebus._
import akka.actor._
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import io.surfkit.telemetry.cluster.UserActor
import io.surfkit.typebus.module.Service
import io.surfkit.typebus.bus.kafka.KafkaBus
import io.surfkit.typebus.event.{EventMeta, ServiceDescriptor}
import io.surfkit.typebus.event._
import scala.concurrent.Future

object DispatchActor{
  // CA - HACK: this is a pretty lame hack to broadcast all the trace messages..
  var adminUserIds = Set.empty[String]
}

class DispatchActor extends Service[Any]("telemetry-dispatch") with Actor with ActorLogging with KafkaBus[Any] with AvroByteStreams{
  implicit val system = context.system
  import system.dispatcher

  val bus = busActor
  val userRegion = ClusterSharding(system). start(
    typeName = "UserActor",
    entityProps = UserActor.props(bus),
    settings = ClusterShardingSettings(system).withRole(serviceName),
    extractEntityId = UserActor.idExtractor,
    extractShardId = UserActor.shardResolver,
  )

  println("Running !!!!")

  def handleServiceDescriptor(x: ServiceDescriptor, meta: EventMeta): Future[Unit] = {
    DispatchActor.adminUserIds.foreach{ uid =>
      userRegion ! UserActor.ShardMessage(UUID.fromString(uid), SocketEvent(
        meta = meta.copy(eventId = UUID.randomUUID().toString),
        payload = serviceDescriptorWriter.write(x)
      ))
    }
    Future.successful(Unit)
  }

  def handleInEventTrace(x: InEventTrace, meta: EventMeta): Future[Unit] = {
    DispatchActor.adminUserIds.foreach{ uid =>
      userRegion ! UserActor.ShardMessage(UUID.fromString(uid), SocketEvent(
        meta = meta.copy(eventId = UUID.randomUUID().toString),
        payload = InEventTraceWriter.write(x)
      ))
    }
    Future.successful(Unit)
  }

  def handleOutEventTrace(x: OutEventTrace, meta: EventMeta): Future[Unit] = {
    DispatchActor.adminUserIds.foreach{ uid =>
      userRegion ! UserActor.ShardMessage(UUID.fromString(uid), SocketEvent(
        meta = meta.copy(eventId = UUID.randomUUID().toString),
        payload = OutEventTraceWriter.write(x)
      ))
    }
    Future.successful(Unit)
  }

  def handleExceptionTrace(x: ExceptionTrace, meta: EventMeta): Future[Unit] = {
    DispatchActor.adminUserIds.foreach{ uid =>
      userRegion ! UserActor.ShardMessage(UUID.fromString(uid), SocketEvent(
        meta = meta.copy(eventId = UUID.randomUUID().toString),
        payload = ExceptionTraceWriter.write(x)
      ))
    }
    Future.successful(Unit)
  }

  override def receive = {
    case _ =>
  }

  registerStream(handleServiceDescriptor _)
  registerStream(handleInEventTrace _)
  registerStream(handleOutEventTrace _)
  registerStream(handleExceptionTrace _)

  startTypeBus
}

