package io.surfkit.typebus.client

import io.surfkit.typebus.{ByteStreamReader, ByteStreamWriter}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.reflect.ClassTag
import akka.util.Timeout
/***
  * Client - base class for generating RPC clients
  * @param system - akka actor system
  */
trait Client{

  /***
    * wire - function to create a Request per Actor and perform the needed type conversions.
    * @param x - This is the request type.  This is of type T.
    * @param timeout - configurable actor timeout with default of 4 seconds
    * @param w - ByteStreamWriter (defaults to avro)
    * @param r - ByteStreamReader (defaults to avro)
    * @tparam T - The IN type for the service call
    * @tparam U - The OUT type in the service called. Wrapped as Future[U]
    * @return - The Future[U] return from the service call.
    */
  def wire[T : ClassTag, U : ClassTag](x: T)(implicit timeout:Timeout = Timeout(4 seconds), w:ByteStreamWriter[T], r: ByteStreamReader[U]) :Future[U]
}


