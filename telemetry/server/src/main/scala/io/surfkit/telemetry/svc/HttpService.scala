package io.surfkit.telemetry.svc

import java.util.UUID

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Directive1, Route}
import org.squbs.unicomplex.RouteDefinition
import akka.actor._
import akka.util.{ByteString, Timeout}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.ws._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl._
import io.surfkit.telemetry.actors.CoreActor
import com.typesafe.config.ConfigFactory
import java.nio.file._

import headers._
import HttpMethods._
import akka.cluster.sharding.ClusterSharding
import io.surfkit.typebus.AvroByteStreams
import io.surfkit.typebus.event._

object ActorPaths {
  // actor path = /user/ + cube-shortname + / + actor name
  val coreActorPath = "/user/telemetry/core"
}

class HttpService extends RouteDefinition with AvroByteStreams {
  implicit val system = context.system
  import system.dispatcher

  lazy val userRegion = ClusterSharding(system).shardRegion("UserActor")

  val decider: Supervision.Decider = {
    case _ => Supervision.Resume  // Never give up !
  }

//  println(s"\n\nschema trace in: \n${InEventTraceReader.schema}\n")
//  println(s"schema trace out: \n${OutEventTraceReader.schema}\n")
//  println(s"ex: \n${ServiceExceptionReader.schema}\n")
//  println(s"Hb: \n${HbWriter.schema}\n")

  val config = ConfigFactory.load()

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  implicit val timeout = Timeout(10 seconds)
  val coreActor = Await.result(system.actorSelection(ActorPaths.coreActorPath).resolveOne(), timeout.duration)

  val wwwPath = config.getString("www.base-url")
  // If wwwPath not found on fs, we force useResourceDir to true
  val useZipFs = config.getBoolean("www.use-zipfs") || Files.notExists(Paths.get(wwwPath))
  val useResourceDir = useZipFs && config.getBoolean("www.use-resource-dir")

  println("CoreHttp is up and running !")


  def getAuthFromToken(token: String): Directive1[String] = {
    onSuccess(Future.successful(Some(token))).flatMap {     // TODO: some kind of auth
      case Some(token) =>
        //println(s"getAuthFromToken got auth for user: ${user}")
        // TODO: make sure we have not expired...
        provide(token)    // TODO: this would return a user id
      case _ =>
        //logger.warn("Rejecting WS connection- token is not present")
        reject(AuthorizationFailedRejection)
    }
  }


  def route: Route =
    options {
      complete(HttpResponse(200).withHeaders(`Access-Control-Allow-Origin`.*, `Access-Control-Allow-Methods`(OPTIONS, POST, PUT, GET, DELETE)))
    } ~
      get {
        pathSingleSlash {
          getFromDirectory(s"${wwwPath}/index.html")
        }
      } ~
      path("p" / Remaining ) { _ =>
        get {
          getFromDirectory(s"${wwwPath}/index.html")
        }
      } ~
      path("static" / Remaining ) { rest =>
        get {
          getFromDirectory(s"${wwwPath}/static/${rest}")
        }
      } ~
      path("bundle.js") {
        get {
          println(s"get bundle.. ${wwwPath}/bundle.js")
          getFromDirectory(s"${wwwPath}/bundle.js")
        }
      } ~
      path("imgs" / Remaining) { (rest) =>
        get {
          getFromDirectory(s"${wwwPath}/imgs/${rest}")
        }
      } ~
      path("v1" / "ws" / JavaUUID) { token =>
        get{
          getAuthFromToken(token.toString) { uuid =>
            println("Handling WS connection")
            handleWebSocketMessages(websocketCoreFlow(sender = token.toString, token = ""))
          }
        }
      }

  def coreInSink(userid: String, unique:UUID) = Sink.actorRef[CoreActor.ReceivedMessage](coreActor, CoreActor.Disconnect(userid,unique))

  def coreFlow(userid: String, token: String, unique:UUID ): Flow[ByteString, SocketEvent, akka.NotUsed] = {
    val uid = UUID.fromString(userid)
    val in =
      Flow[ByteString]
        .map{x =>
          println(s"Get the message: ${x}")
          try{
            val socketEvent = socketEventReader.read(x.toArray)
            println(s"coreFlow Event Type: ${socketEvent.meta.eventType}")
            CoreActor.ReceivedMessage(userid, unique, socketEvent)
          }catch{
            case t: Throwable =>
              t.printStackTrace()
              throw t
          }
        }
        .to(coreInSink(userid, unique))
    // The counter-part which is a source that will create a target ActorRef per
    // materialization where the coreActor will send its messages to.
    // This source will only buffer n element and will fail if the client doesn't read
    // messages fast enough.
    val out = Source.actorRef[SocketEvent](32, OverflowStrategy.fail)
      .mapMaterializedValue(coreActor ! CoreActor.Connect(userid, unique, _, token))
    Flow.fromSinkAndSource(in, out)//(Keep.none)
  }


  def websocketCoreFlow(sender: String, token: String): Flow[Message, Message, akka.NotUsed] = {
    Flow[Message]
      .collect {
        case TextMessage.Strict(msg) ⇒
          println(s"WebSocket got text: ${msg} ??")
          Future.successful(ByteString.empty)
        case TextMessage.Streamed(stream) =>
          stream.runWith(Sink.ignore)
          Future.successful(ByteString.empty)
        case BinaryMessage.Strict(msg) =>
          println(s"WebSocket Got binary")
          //other.dataStream.runWith(Sink.ignore)
          Future.successful(msg)
        case BinaryMessage.Streamed(stream) =>
          stream
            .limit(100) // Max frames we are willing to wait for
            .completionTimeout(5 seconds) // Max time until last frame
            .runFold(ByteString.empty)(_ ++ _) // Merges the frames
            .flatMap(msg => Future.successful(msg))
      }.mapAsync(parallelism = 3)(identity)
      .via(coreFlow(sender, token, UUID.randomUUID())) // ... and route them through the chatFlow ...
      // client now sends this..
      /*.keepAlive(45 seconds, () =>
        SocketEvent(
          correlationId = UUID.randomUUID().toString,
          occurredAt = new org.joda.time.DateTime(),
          payload = m.KeepAlive
        )
      )*/
      .map {
        case x: SocketEvent => {
          //logger.info(s"DOWN THE SOCKET: ${x.payload.getClass.getName}")
          try {
            println(s"DOWN THE SOCKET: ${x.meta.eventType}")
            BinaryMessage.Strict(ByteString(socketEventWriter.write(x)))
          } catch {
            case t: Throwable =>
              println(s"ERROR: trying to unpickle type: : ${x}", t)
              throw t
          }
        }
    }
  }

}

