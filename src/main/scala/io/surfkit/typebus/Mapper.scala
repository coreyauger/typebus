package io.surfkit.typebus

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