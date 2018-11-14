package io.surfkit.typebus

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s._

/***
  * Schemacha - cheeky name for Schema wrapper
  */
trait Schemacha{
  def schema: String
}

/***
  * ByteStreamWriter - define a class that knows how to serialize a type A
  * @tparam A - the type to be serialized
  */
trait ByteStreamWriter[A] extends Schemacha{
  /***
    * write - take a type A to Array[Byte]
    * @param value of type A
    * @return Array[Byte]
    */
  def write(value: A): Array[Byte]
}

/***
  * ByteStreamReader - define a class that knows how to read Array[Byte] and make a type A
  * @tparam A - the type A
  */
trait ByteStreamReader[A] extends Schemacha{
  /***
    * read - take an Array[Byte] and make a type A
    * @param bytes - Array[Byte]
    * @return - a type A
    */
  def read(bytes: Array[Byte]): A
}

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
  class AvroByteStreamReader[T : SchemaFor : Decoder] extends ByteStreamReader[T]{
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
  class AvroByteStreamWriter[T : SchemaFor : Encoder] extends ByteStreamWriter[T]{
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
}


// CA: FUTURE: we could allow for json as well..
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
      val input = AvroInputStream.json[T].from(json).build(avroSchema)
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

