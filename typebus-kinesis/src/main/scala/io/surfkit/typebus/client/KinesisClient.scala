package io.surfkit.typebus.client

import java.nio.ByteBuffer

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.OverflowStrategy.backpressure
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.alpakka.kinesis.KinesisFlowSettings
import akka.stream.alpakka.kinesis.scaladsl.KinesisSink
import akka.stream.scaladsl.Source
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}
import com.typesafe.config.ConfigFactory
import io.surfkit.typebus.{AvroByteStreams, ByteStreamReader, ByteStreamWriter}
import io.surfkit.typebus.actors.GatherActor
import io.surfkit.typebus.bus.Publisher
import io.surfkit.typebus.event.PublishedEvent

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag

/***
  * Client - base class for generating RPC clients
  * @param system - akka actor system
  */
class KinesisClient(implicit system: ActorSystem) extends Client with Publisher with AvroByteStreams{
  import akka.pattern.ask
  import akka.util.Timeout
  import system.dispatcher


  import collection.JavaConverters._
  val cfg = ConfigFactory.load
  val kinesisStream = cfg.getString("bus.kinesis.stream")
  val kinesisEndpoint = cfg.getString("bus.kinesis.endpoint")
  val kinesisRegion = cfg.getString("bus.kinesis.region")
  val shards = cfg.getStringList("bus.kinesis.shards").asScala
  val kinesisPartitionKey = shards( Math.floor(Math.random() * shards.size).toInt )    // pick a random shard to publish?

  val decider: Supervision.Decider = {
    case _ => Supervision.Resume  // Never give up !
  }
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  // Create a Kinesis endpoint pointed at our local kinesalite
  val endpoint = new EndpointConfiguration(kinesisEndpoint, kinesisRegion)
  implicit val amazonKinesisAsync: AmazonKinesisAsync = AmazonKinesisAsyncClientBuilder.standard().withEndpointConfiguration(endpoint).build()

  val tyebusMap = scala.collection.mutable.Map.empty[String, ActorRef]

  val publishedEventReader = new AvroByteStreamReader[PublishedEvent]
  val publishedEventWriter = new AvroByteStreamWriter[PublishedEvent]

  //#flow-settings
  val flowSettings = KinesisFlowSettings(
    parallelism = 1,
    maxBatchSize = 500,
    maxRecordsPerSecond = 1000,
    maxBytesPerSecond = 1000000,
    maxRetries = 5,
    backoffStrategy = KinesisFlowSettings.Exponential,
    retryInitialTimeout = 100.millis
  )

  val publishActor = Source.actorRef[PublishedEvent](Int.MaxValue, backpressure)
    .map(event => publishedEventWriter.write(event))
    .map(data => new PutRecordsRequestEntry().withData( ByteBuffer.wrap(data) ).withPartitionKey(kinesisPartitionKey))
    .to(KinesisSink(kinesisStream, flowSettings))
    .run()

  def publish(event: PublishedEvent): Unit =
    try {
      system.log.info(s"[KafkaClient] publish ${event.meta.eventType}")
      publishActor ! event
    }catch{
      case e:Exception =>
        system.log.error(e, "Error trying to publish event.")
    }


  /***
    * wire - function to create a Request per Actor and perform the needed type conversions.
    * @param x - This is the request type.  This is of type T.
    * @param timeout - configurable actor timeout with default of 4 seconds
    * @param w - ByteStreamWriter (defaults to avro)
    * @param r - ByteStreamReader (defaults to avro)
    * @tparam T - The IN type for the service call
    * @tparam U - The OUT type in the service called. Wrapped as Future[U]
    * @return - The Future[U] return from the service call.
    */
  def wire[T : ClassTag, U : ClassTag](x: T)(implicit timeout:Timeout = Timeout(4 seconds), w:ByteStreamWriter[T], r: ByteStreamReader[U]) :Future[U] = {
    val gather = system.actorOf(Props(new GatherActor[T, U](this, timeout, w, r)))
    (gather ? GatherActor.Request(x)).map(_.asInstanceOf[U])
  }
}


