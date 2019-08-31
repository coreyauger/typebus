package io.surfkit.typebus.entity

import io.surfkit.typebus.{ByteStreamReader, ByteStreamWriter}

import scala.concurrent.Future
import scala.reflect.ClassTag

abstract class StatefulEntity[S : ClassTag : ByteStreamReader : ByteStreamWriter] {
  // TODO: ..
}

trait CQRSDatabase[S]{
  def getState(id: String): Future[S]
  // TODO compensating action
  //def modifyState(id: String, state: S):
}
