package io.surfkit.cluster

import com.typesafe.scalalogging.LazyLogging
import org.squbs.lifecycle.ExtensionLifecycle
import org.squbs.util.ConfigUtil
import akka.cluster.seed.ZookeeperClusterSeed
import org.squbs.unicomplex.UnicomplexBoot.StartupType

import scala.concurrent.duration._

object ZkClusterExtension{
  var zkInit = false
}

private class ZkClusterExtension extends ExtensionLifecycle with LazyLogging{
  private object Locker

  override def postInit(): Unit = {
    logger.info(s"ZkClusterExtension::postInit ${this.getClass}")
    import ConfigUtil._
    import boot._

    implicit val system = boot.actorSystem
    if(!ZkClusterExtension.zkInit)
      Locker.synchronized {
        ZkClusterExtension.zkInit = true
        logger.info("Trying to join cluster.")
        ZookeeperClusterSeed(system).join()
      }
  }
}
