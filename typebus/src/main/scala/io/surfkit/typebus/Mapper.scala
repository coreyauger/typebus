package io.surfkit.typebus

import com.sksamuel.avro4s._
import io.surfkit.typebus.event._
import scala.reflect.ClassTag
import java.io.ByteArrayOutputStream

/***
  * AvroByteStreams - defualt type serialization for typebus
  */
trait AvroByteStreams{

  /***
    * AvroByteStreamReader - Take a type T and deserialize from bytes also storing the Schema
    * @param ev$1 - SchemaFor
    * @param ev$2 - Decoder
    * @tparam T - the type T
    */
  class AvroByteStreamReader[T : SchemaFor : Decoder : ClassTag] extends ByteStreamReader[T]{
    val avroSchema = AvroSchema[T]

    /***
      * read - read avro byte array and convert to a tyep T
      * @param bytes - Array[Byte]
      * @return - a type A
      */
    override def read(bytes: Array[Byte]): T = {
      val input = AvroInputStream.binary[T].from(bytes).build(avroSchema)
      val result = input.iterator.toSeq
      result.head
    }

    /***
      * schema
      * @return - avro schema string
      */
    override def schema: String = avroSchema.toString
  }

  /***
    * AvroByteStreamWriter - Take a type T and serialize to byte also storing the Schema
    * @param ev$1 - SchemaFor
    * @param ev$2 - Encoder
    * @tparam T - tye type T
    */
  class AvroByteStreamWriter[T : SchemaFor : Encoder : ClassTag] extends ByteStreamWriter[T]{
    val avroSchema = AvroSchema[T]

    /***
      * write - take a type T and write avro bytes
      * @param obj - the type to write
      * @return Array[Byte]
      */
    override def write(obj: T): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[T].to(baos).build(avroSchema)
      output.write(obj)
      output.close()
      baos.toByteArray
    }

    /***
      * schema
      * @return - avro schema string
      */
    override def schema: String = avroSchema.toString
  }

  implicit val HbReader = new AvroByteStreamReader[Hb]
  implicit val HbWriter = new AvroByteStreamWriter[Hb]
  implicit val ServiceExceptionReader = new AvroByteStreamReader[ServiceException]
  implicit val ServiceExceptionWriter = new AvroByteStreamWriter[ServiceException]
  implicit val publishedEventReader = new AvroByteStreamReader[PublishedEvent]
  implicit val publishedEventWriter = new AvroByteStreamWriter[PublishedEvent]
  implicit val socketEventReader = new AvroByteStreamReader[SocketEvent]
  implicit val socketEventWriter = new AvroByteStreamWriter[SocketEvent]
  implicit val serviceIdentifierWriter = new AvroByteStreamWriter[ServiceIdentifier]
  implicit val serviceIdentifierReader = new AvroByteStreamReader[ServiceIdentifier]
  implicit val serviceDescriptorWriter = new AvroByteStreamWriter[ServiceDescriptor]
  implicit val serviceDescriptorReader = new AvroByteStreamReader[ServiceDescriptor]
  implicit val getServiceDescriptorReader = new AvroByteStreamReader[GetServiceDescriptor]
  implicit val getServiceDescriptorWriter = new AvroByteStreamWriter[GetServiceDescriptor]
  implicit val OutEventTraceReader = new AvroByteStreamReader[OutEventTrace]
  implicit val OutEventTraceWriter = new AvroByteStreamWriter[OutEventTrace]
  implicit val InEventTraceReader = new AvroByteStreamReader[InEventTrace]
  implicit val InEventTraceWriter = new AvroByteStreamWriter[InEventTrace]
  implicit val ExceptionTraceReader = new AvroByteStreamReader[ExceptionTrace]
  implicit val ExceptionTraceWriter = new AvroByteStreamWriter[ExceptionTrace]

}


trait JsonStreamWriter[A] extends Schemacha{
  def write(value: A): String
}

trait JsonStreamReader[A] extends Schemacha{
  def read(json: String): A
}

trait AvroJsonStream{

  class AvroJsonStreamReader[T : SchemaFor : Decoder] extends JsonStreamReader[T] {
    val avroSchema = AvroSchema[T]

    override def read(json: String): T = {
      val input = AvroInputStream.json[T].from(json.getBytes).build(avroSchema)
      val result = input.iterator.toSeq
      result.head
    }

    override def schema: String = avroSchema.toString
  }

  class AvroJsonStreamWriter[T : SchemaFor : Encoder] extends JsonStreamWriter[T]{
    val avroSchema = AvroSchema[T]

    override def write(obj: T): String = {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.json[T].to(baos).build(avroSchema)
      output.write(obj)
      output.close()
      baos.toString("UTF-8")
    }

    override def schema: String = avroSchema.toString
  }

}


