package io.surfkit.typebus

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s._

trait ByteStreamWriter[A]{
  def write(value: A): Array[Byte]
}

trait ByteStreamReader[A]{
  def read(bytes: Array[Byte]): A
}

object Mapper{
  def toByteStream[A](value: A)(implicit writer: ByteStreamWriter[A]): Array[Byte] = writer.write(value)
  def fromByteStream[A](bytes: Array[Byte])(implicit reader: ByteStreamReader[A]): A = reader.read(bytes)
}


trait AvroByteStreams{

  class AvroByteStreamReader[T : SchemaFor : Decoder] extends ByteStreamReader[T]{
    val schema = AvroSchema[T]
    override def read(bytes: Array[Byte]): T = {
      val input = AvroInputStream.binary[T].from(bytes).build(schema)
      val result = input.iterator.toSeq
      result.head
    }
  }
  class AvroByteStreamWriter[T : SchemaFor : Encoder] extends ByteStreamWriter[T]{
    val schema = AvroSchema[T]
    override def write(obj: T): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      val output = AvroOutputStream.binary[T].to(baos).build(schema)
      output.write(obj)
      output.close()
      baos.toByteArray
    }
  }
}