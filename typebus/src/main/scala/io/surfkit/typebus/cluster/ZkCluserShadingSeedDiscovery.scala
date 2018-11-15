package io.surfkit.typebus.cluster

import akka.actor.ActorSystem
import akka.cluster.seed.ZookeeperClusterSeed

trait ZkCluserShadingSeedDiscovery{
  def zkCluserSeed(system: ActorSystem) = ZookeeperClusterSeed(system)
}