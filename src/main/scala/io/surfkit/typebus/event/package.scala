package io.surfkit.typebus

import org.joda.time.DateTime

import java.util.UUID
import scala.annotation.implicitNotFound
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import com.sksamuel.avro4s.{RecordFormat, SchemaFor, ToRecord, FromRecord}

package object event {

  sealed trait TypeBus{}

  case class EventMeta(eventId: String,
                       eventType: String,
                       source: String,
                       correlationId: Option[String],
                       userId: Option[String] = None,
                       socketId: Option[String] = None,
                       responseTo: Option[String] = None,
                       /*occurredAt: DateTime*/) extends TypeBus


  final case class SocketEvent(
                             meta: EventMeta,
                             payload: Array[Byte]
                           ) extends TypeBus

  final case class PublishedEvent(
                             meta: EventMeta,
                             payload: Array[Byte]
                              ) extends TypeBus


  //implicit val schemaEventMeta = AvroSchema[EventMeta]
  //implicit val formatEventMeta = RecordFormat[EventMeta]

  //implicit val schemaSocket = AvroSchema[SocketEvent]
  //implicit val formatSocket = RecordFormat[SocketEvent]

  //implicit val schemaPublish = AvroSchema[PublishedEvent]
  //implicit val formatPublish = RecordFormat[PublishedEvent]
}






