package io.surfkit.typebus.actors

import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import io.surfkit.typebus.event.PublishedEvent


object TypeBusActor {
  def props(`type`: String, parent: ActorRef): Props = Props(classOf[TypeBusActor], `type`, parent)

  case class Message(event: PublishedEvent)
}

class TypeBusActor(`type`: String, parent: ActorRef) extends Actor {
  import akka.cluster.pubsub.DistributedPubSubMediator.Publish
  context.system.log.info(s"create TypeBusActor actor for type ${`type`}")
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe(`type`, self)
  context.system.log.info(s"${`type`} joining DistributedPubSub mediator")

  def receive = {
    case event: PublishedEvent =>
      context.system.log.info(s"[${`type`}] Got a publish event message: ${TypeBusActor.Message(event)}")
      mediator ! Publish(`type`, TypeBusActor.Message(event) )

    case TypeBusActor.Message(event) =>
      context.system.log.info(s"[${`type`}] Got a subscription message.. sending to parent: ${event}")
      println("!!!!!!")
      parent ! event

    case x: SubscribeAck ⇒
      context.system.log.info(s"[${`type`}] SUBSCRIBED: ${x}")
  }
}