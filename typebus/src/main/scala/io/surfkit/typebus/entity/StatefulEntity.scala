package io.surfkit.typebus.entity

import io.surfkit.typebus.{ByteStreamReader, ByteStreamWriter}

import scala.reflect.ClassTag

abstract class StatefulEntity[S : ClassTag : ByteStreamReader : ByteStreamWriter] {
  // TODO: ..
}
