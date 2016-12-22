package io.surfkit.typebus

import scala.reflect.ClassTag

/**
  * Created by suroot on 21/12/16.
  */
trait Mapper {
  def writeValueAsString[T](value: T): String
  def readValue[T : ClassTag](content: String): T
}
