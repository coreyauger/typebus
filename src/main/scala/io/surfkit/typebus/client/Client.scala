package io.surfkit.typebus.client

import akka.actor.ActorSystem
import io.surfkit.typebus.actors.GatherActor
import org.apache.kafka.clients.producer.KafkaProducer
import io.surfkit.typebus.macros.TypeBusClient

import scala.concurrent.Future
import scala.concurrent.duration._

trait ApiClient[Api]
/**
  * Created by suroot on 21/12/16.
  */

class Client[Api : Manifest](mapper: io.surfkit.typebus.Mapper, host: String)(implicit system: ActorSystem){
  import akka.pattern.ask
  import collection.JavaConversions._
  import akka.util.Timeout
  import system.dispatcher

  val producer = new KafkaProducer[Array[Byte], String](Map(
    "bootstrap.servers" -> host,
    "key.serializer" ->  "org.apache.kafka.common.serialization.ByteArraySerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
  ))

  implicit val timeout = Timeout(4 seconds)

  def wire[T <: m.Model, U <: m.Model](x: T):Future[U] = {
    val gather = system.actorOf(GatherActor.props(producer, mapper))
    (gather ? GatherActor.Request(x)).map(_.asInstanceOf[U])
  }

  //@TypeBusClient("")
  //object api extends ApiClient[Api]

}
