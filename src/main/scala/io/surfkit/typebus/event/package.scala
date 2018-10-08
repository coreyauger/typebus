package io.surfkit.typebus

/**
  * Created by suroot on 21/12/16.
  */
import org.joda.time.DateTime

import java.util.UUID
import scala.annotation.implicitNotFound
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag

package object event {

  /*case class Event[T](
                       source: String,
                       userIdentifier: Option[String],
                       correlationId: Option[String],
                       occurredAt: DateTime,
                       payload: T
                     )*/

  sealed trait Tb{}

  final case class SocketEvent[T](
                             correlationId: String,
                             occurredAt: DateTime,
                             payload: T
                           ) extends Tb

  final case class PublishedEvent[T](
                                eventId: String,
                                eventType: String,
                                source: String,
                                userIdentifier: Option[String],
                                socketId: Option[String],
                                correlationId: Option[String],
                                occurredAt: DateTime,
                                publishedAt: DateTime,
                                payload: T
                              ) extends Tb{

    def toReply[U](payload: U, eventId: String = UUID.randomUUID().toString) = ResponseEvent(
      eventId = eventId,
      eventType = payload.getClass.getCanonicalName,
      source = this.source,
      userIdentifier = this.userIdentifier,
      socketId = this.socketId,
      correlationId = this.correlationId,
      occurredAt = this.occurredAt,
      publishedAt = this.publishedAt,
      payload = payload
    )

    def toBroadcastReply[U](payload: U, eventId: String = UUID.randomUUID().toString) = ResponseEvent(
      eventId = eventId,
      eventType = payload.getClass.getCanonicalName,
      source = this.source,
      userIdentifier = this.userIdentifier,
      socketId = None,
      correlationId = this.correlationId,
      occurredAt = this.occurredAt,
      publishedAt = this.publishedAt,
      payload = payload
    )

    def toEvent[U](payload: U, eventId: String = UUID.randomUUID().toString) = PublishedEvent(
      eventId = eventId,
      eventType = payload.getClass.getCanonicalName,
      source = this.source,
      userIdentifier = this.userIdentifier,
      socketId = this.socketId,
      correlationId = this.correlationId,
      occurredAt = this.occurredAt,
      publishedAt = this.publishedAt,
      payload = payload
    )
  }

  case class ResponseEvent[T](
                                eventId: String,
                                eventType: String,
                                source: String,
                                userIdentifier: Option[String],
                                socketId: Option[String],
                                correlationId: Option[String],
                                occurredAt: DateTime,
                                publishedAt: DateTime,
                                payload: T
                              ) extends Tb


}






