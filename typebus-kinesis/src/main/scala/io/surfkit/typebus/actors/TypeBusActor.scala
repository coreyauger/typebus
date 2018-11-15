package io.surfkit.typebus.actors

import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.client.ClusterClient.Publish
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.Subscribe
import io.surfkit.typebus.event.PublishedEvent


object TypeBusActor {
  def props(`type`: String, parent: ActorRef): Props = Props(classOf[TypeBusActor], `type`, parent)

  case class Message(event: PublishedEvent)
}

class TypeBusActor(`type`: String, parent: ActorRef) extends Actor {
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! Subscribe(`type`, self)
  context.system.log.info(s"${`type`} joining DistributedPubSub mediator")

  def receive = {
    case event: PublishedEvent =>
      mediator ! Publish(`type`, event)

    case TypeBusActor.Message(event) =>
      parent ! event
  }
}