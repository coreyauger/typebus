package io.surfkit.telemetry.svc

import java.util.UUID

import io.surfkit.typebus._
import akka.actor._
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import io.surfkit.telemetry.cluster.UserActor
import io.surfkit.typebus.module.Service
import io.surfkit.typebus.bus.Publisher
import io.surfkit.typebus.annotations.ServiceMethod
import io.surfkit.typebus.event._

import scala.concurrent.Future



object TelemetryService{
  // CA - HACK: this is a pretty lame hack to broadcast all the trace messages..
  var adminUserIds = Set.empty[String]
}

class TelemetryService(serviceIdentifier: ServiceIdentifier, publisher: Publisher, sys: ActorSystem)
  extends Service(serviceIdentifier,publisher) with AvroByteStreams{
  implicit val system = sys

  val bus = publisher.busActor
  val userRegion = ClusterSharding(system).start(
    typeName = "UserActor",
    entityProps = UserActor.props(bus),
    settings = ClusterShardingSettings(system).withRole(serviceIdentifier.name),
    extractEntityId = UserActor.idExtractor,
    extractShardId = UserActor.shardResolver
  )

  @ServiceMethod
  def handleServiceDescriptor(x: ServiceDescriptor, meta: EventMeta): Future[Unit] = {
    TelemetryService.adminUserIds.foreach{ uid =>
      userRegion ! UserActor.ShardMessage(UUID.fromString(uid), SocketEvent(
        meta = meta.copy(eventId = UUID.randomUUID().toString),
        payload = serviceDescriptorWriter.write(x)
      ))
    }
    Future.successful(Unit)
  }
  registerStream(handleServiceDescriptor _)

  @ServiceMethod
  def handleInEventTrace(x: InEventTrace, meta: EventMeta): Future[Unit] = {
    TelemetryService.adminUserIds.foreach{ uid =>
      userRegion ! UserActor.ShardMessage(UUID.fromString(uid), SocketEvent(
        meta = meta.copy(eventId = UUID.randomUUID().toString),
        payload = InEventTraceWriter.write(x)
      ))
    }
    Future.successful(Unit)
  }
  registerStream(handleInEventTrace _)

  @ServiceMethod
  def handleOutEventTrace(x: OutEventTrace, meta: EventMeta): Future[Unit] = {
    TelemetryService.adminUserIds.foreach{ uid =>
      userRegion ! UserActor.ShardMessage(UUID.fromString(uid), SocketEvent(
        meta = meta.copy(eventId = UUID.randomUUID().toString),
        payload = OutEventTraceWriter.write(x)
      ))
    }
    Future.successful(Unit)
  }
  registerStream(handleOutEventTrace _)

  @ServiceMethod
  def handleExceptionTrace(x: ExceptionTrace, meta: EventMeta): Future[Unit] = {
    TelemetryService.adminUserIds.foreach{ uid =>
      userRegion ! UserActor.ShardMessage(UUID.fromString(uid), SocketEvent(
        meta = meta.copy(eventId = UUID.randomUUID().toString),
        payload = ExceptionTraceWriter.write(x)
      ))
    }
    Future.successful(Unit)
  }
  registerStream(handleExceptionTrace _)

}

