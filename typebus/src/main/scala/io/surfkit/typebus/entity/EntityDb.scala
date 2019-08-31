package io.surfkit.typebus.entity

import io.surfkit.typebus.bus.Publisher

import scala.concurrent.Future
import scala.reflect.ClassTag
import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.scaladsl.{EntityContext, EntityTypeKey}
import io.surfkit.typebus.event.EntityCreated
import io.surfkit.typebus.AvroByteStreams

trait EntityDb[S] extends AvroByteStreams{

  def producer: Publisher
  def typeKey: EntityTypeKey[_]

  io.surfkit.typebus.module.Service.resisterEntity(this)

  def createEntity[M](behavior: String => Behavior[M])(implicit system: akka.actor.ActorSystem): EntityContext => Behavior[M] =
    { ctx: EntityContext =>
      producer.publish(EntityCreated(typeKey.name, ctx.entityId))
      behavior(ctx.entityId)
    }



  def getState(id: String): Future[S]
  def modifyState(id: String, state: S): Future[S]
}
