package io.surfkit.typebus.bus.kinesis

import java.nio.ByteBuffer
import java.security.cert.X509Certificate
import java.util.UUID

import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream.alpakka.kinesis.{KinesisFlowSettings, ShardSettings}
import akka.stream.OverflowStrategy.fail
import akka.stream.alpakka.kinesis.scaladsl.{KinesisFlow, KinesisSink, KinesisSource}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.util.Timeout
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentialsProviderChain, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.{AmazonKinesisAsync, AmazonKinesisAsyncClientBuilder}
import com.amazonaws.services.kinesis.model.{PutRecordsRequestEntry, Record, ShardIteratorType}
import com.typesafe.config.ConfigFactory
import io.surfkit.typebus.AvroByteStreams
import io.surfkit.typebus.actors.{ProducerActor, TypeBusActor}
import io.surfkit.typebus.bus.Bus
import io.surfkit.typebus.event._
import io.surfkit.typebus.module.Service
import javax.net.ssl.{KeyManager, SSLContext, X509TrustManager}
import org.apache.http.conn.ssl.{NoopHostnameVerifier, SSLConnectionSocketFactory}

import scala.concurrent.duration._
import scala.concurrent.Future

trait KinesisBus[UserBaseType] extends Bus[UserBaseType] with AvroByteStreams with Actor {
  service: Service[UserBaseType] =>

  import collection.JavaConverters._
  import context.dispatcher
  val cfg = ConfigFactory.load
  val kinesisStream = cfg.getString("bus.kinesis.stream")
  val kinesisEndpoint = cfg.getString("bus.kinesis.endpoint")
  val kinesisRegion = cfg.getString("bus.kinesis.region")
  val shards = cfg.getStringList("bus.kinesis.shards").asScala

  val decider: Supervision.Decider = {
    case _ => Supervision.Resume  // Never give up !
  }
  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(context.system).withSupervisionStrategy(decider))


  // Create a Kinesis endpoint pointed at our local kinesalite
  val endpoint = new EndpointConfiguration(kinesisEndpoint, kinesisRegion)
  implicit val amazonKinesisAsync: AmazonKinesisAsync =
    AmazonKinesisAsyncClientBuilder.standard()
      // CA - WARNING we only want to ignore SSL validation on localhost
      .withClientConfiguration(ignoringInvalidSslCertificates(new ClientConfiguration()))
      .withEndpointConfiguration(endpoint)
      .build()
  context.system.registerOnTermination(amazonKinesisAsync.shutdown())

  val tyebusMap = scala.collection.mutable.Map.empty[String, ActorRef]

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

  val publishActor = Source.actorRef[PublishedEvent](Int.MaxValue, fail)
    .map(event => (publishedEventWriter.write(event),event.meta) )
    .map{ case (data, meta) =>
      println(s"kinesis publish actor got data with size: ${data.size} to withPartitionKey: ${meta.eventType} ")
      new PutRecordsRequestEntry().withData( ByteBuffer.wrap(data) ).withPartitionKey("typebus")
    }
    .to(KinesisSink(kinesisStream, flowSettings))
    .run()

  def publish(event: PublishedEvent): Unit = {
    println("sending event to kinesis publish actor")
    publishActor ! event
  }

  def busActor(implicit system: ActorSystem): ActorRef =
    system.actorOf(Props(new ProducerActor(this)))

  def startTypeBus(serviceName: String)(implicit system: ActorSystem): Unit = {
    import system.dispatcher

    val serviceDescription = makeServiceDescriptor(serviceName)
    implicit val serviceDescriptorWriter = new AvroByteStreamWriter[ServiceDescriptor]
    implicit val getServiceDescriptorReader = new AvroByteStreamReader[GetServiceDescriptor]

    /***
      * getServiceDescriptor - default hander to broadcast ServiceDescriptions
      * @param x - GetServiceDescriptor is another service requesting a ServiceDescriptions
      * @param meta - EventMeta routing info
      * @return - Future[ServiceDescriptor]
      */
    def getServiceDescriptor(x: GetServiceDescriptor, meta: EventMeta): Future[ServiceDescriptor] = {
      println("**************** getServiceDescriptor")
      println(serviceDescription)
      Future.successful(serviceDescription)
    }
    registerServiceStream(getServiceDescriptor _)

    tyebusMap ++= (listOfFunctions.map(_._1) ::: listOfServiceFunctions.map(_._1)).map(x => x -> context.actorOf(TypeBusActor.props(x, context.self)) ).toMap
    context.system.log.info(s"tyebusMap: ${tyebusMap}")


    val consumerConfig = new KinesisClientLibConfiguration(
      "typebus",
      kinesisStream,
      new DefaultAWSCredentialsProviderChain,
      "kinesisWorker"
    )
      // CA - WARNING we only want to ignore SSL validation on localhost
      .withCommonClientConfig(ignoringInvalidSslCertificates(new ClientConfiguration()))
      .withCallProcessRecordsEvenForEmptyRecordList(true)
      .withRegionName(kinesisRegion)

      .withDynamoDBEndpoint("http://localhost:8000")
      .withKinesisEndpoint(kinesisEndpoint.toString)
      .withInitialPositionInStream(InitialPositionInStream.LATEST)

    case class KeyMessage(key: String, data: Array[Byte], markProcessed: () => Unit)

    val atLeastOnceSource = com.contxt.kinesis.KinesisSource(consumerConfig)

    println("RUNNING !!!!")
    println("#######################################################################")
    println("#######################################################################")
    atLeastOnceSource.map { kinesisRecord =>
        println(s"kinesisRecord : ${kinesisRecord}")
        KeyMessage(
          kinesisRecord.partitionKey, kinesisRecord.data.toArray, kinesisRecord.markProcessed
        )
      }
      .map { message =>
        println(s"GOT mergedSource: ${message}")
        val event = publishedEventReader.read(message.data)
        if(!tyebusMap.contains(event.meta.eventType) )
          tyebusMap += event.meta.eventType -> context.actorOf(TypeBusActor.props(event.meta.eventType, context.self))
        tyebusMap(event.meta.eventType) ! event
        event
        // After a record is marked as processed, it is eligible to be checkpointed in DynamoDb.
        message.markProcessed()
        message
      }.runWith(Sink.ignore)

    /*
    // source-list
    val mergeSettings = shards.map { shardId =>
      ShardSettings(kinesisStream,
        shardId,
        ShardIteratorType.LATEST,
        refreshInterval = 1.second,
        limit = 500
        //, consumerName = Some("typebus")
      )
    }.toList

    val mergedSource: Source[Record, NotUsed] = KinesisSource.basicMerge(mergeSettings, amazonKinesisAsync)

    mergedSource.map { msg =>
      println(s"GOT mergedSource: ${msg}")
      val event = publishedEventReader.read(msg.getData.array())
      if(!tyebusMap.contains(event.meta.eventType) )
        tyebusMap += event.meta.eventType -> context.actorOf(TypeBusActor.props(event.meta.eventType, context.self))
      tyebusMap(event.meta.eventType) ! event
      event
    }.runWith(Sink.ignore)
    */
  }

  def receive: Receive = {
    case event: PublishedEvent =>
      context.system.log.debug(s"TypeBus: got msg for topic: ${event.meta.eventType}")
      try {
        println(s"listOfServiceImplicitsReaders: ${listOfServiceImplicitsReaders}")
        println(s"listOfImplicitsReaders: ${listOfImplicitsReaders}")
        val reader = listOfServiceImplicitsReaders.get(event.meta.eventType).getOrElse(listOfImplicitsReaders(event.meta.eventType))
        context.system.log.debug(s"TypeBus: got publish: ${event}")
        context.system.log.debug(s"TypeBus: reader: ${reader}")
        context.system.log.debug(s"publish.payload.size: ${event.payload.size}")
        val payload = reader.read(event.payload)
        context.system.log.debug(s"TypeBus: got payload: ${payload}")
        (if(handleEventWithMetaUnit.isDefinedAt( (payload, event.meta) ) )
          handleEventWithMetaUnit( (payload, event.meta) )
        else if(handleEventWithMeta.isDefinedAt( (payload, event.meta) ) )
          handleEventWithMeta( (payload, event.meta)  )
        else if(handleServiceEventWithMeta.isDefinedAt( (payload, event.meta) ) )
          handleServiceEventWithMeta( (payload, event.meta) )
        else
          handleEvent(payload)) map{ ret =>
          implicit val timeout = Timeout(4 seconds)
          val retType = ret.getClass.getCanonicalName
          val publishedEvent = PublishedEvent(
            meta = event.meta.copy(
              eventId = UUID.randomUUID.toString,
              eventType = ret.getClass.getCanonicalName,
              responseTo = Some(event.meta.eventId)
            ),
            payload = listOfServiceImplicitsWriters.get(retType).map{ writer =>
              writer.write(ret.asInstanceOf[TypeBus])
            }.getOrElse(listOfImplicitsWriters(retType).write(ret.asInstanceOf[UserBaseType]))
          )
          event.meta.directReply.foreach( context.system.actorSelection(_).resolveOne().map( actor => actor ! publishedEvent ) )
          publish(publishedEvent)
        }
      }catch{
        case t:Throwable =>
          t.printStackTrace()
          throw t
      }
  }





  // CA - code based off: https://www.programcreek.com/java-api-examples/?code=adobe/S3Mock/S3Mock-master/src/test/java/com/adobe/testing/s3mock/its/AmazonClientUploadIT.java
  private def ignoringInvalidSslCertificates(clientConfiguration: ClientConfiguration) = {
    clientConfiguration.getApacheHttpClientConfig()
      .withSslSocketFactory(new SSLConnectionSocketFactory(
        createBlindlyTrustingSSLContext(),
        NoopHostnameVerifier.INSTANCE))
    clientConfiguration
  }


  private def createBlindlyTrustingSSLContext(): SSLContext = {
    object WideOpenX509TrustManager extends X509TrustManager {
      override def checkClientTrusted(chain: Array[X509Certificate], authType: String) = ()
      override def checkServerTrusted(chain: Array[X509Certificate], authType: String) = ()
      override def getAcceptedIssuers = Array[X509Certificate]()
    }

    val context = SSLContext.getInstance("TLS")
    context.init(Array[KeyManager](), Array(WideOpenX509TrustManager), null)
    context

  }
}
