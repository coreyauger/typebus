package io.surfkit.typebus

/**
  * Created by suroot on 21/12/16.
  */
import org.joda.time.DateTime

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

  case class PublishedEvent[T](
                                eventId: String,
                                eventType: String,
                                source: String,
                                userIdentifier: Option[String],
                                correlationId: Option[String],
                                occurredAt: DateTime,
                                publishedAt: DateTime,
                                payload: T
                              ) extends m.Model


 // trait RpcCall[T <: m.Model, U <: m.Model]{
    //val ttag: ClassTag[U]
    //val result = Promise[ttag.runtimeClass.asInstanceOf[Class[U]]]
  //}
   /*@implicitNotFound(
     "No Rpc definition found for type ${T}. Try to implement an implicit RpcCall for this type."
   )
  class RpcCall[T <: m.Model, U <: m.Model](implicit val ttag: ClassTag[U]){
    val ctype = ttag.runtimeClass.asInstanceOf[Class[U]]

    def result: Promise[U] = {
      Promise.getClass.getConstructors.head.newInstance(null, ttag).asInstanceOf[Promise[U]]
    }//Promise[Any]().asInstanceOf[Promise[U]]
    //val result = Promise[ctype.getClass]()
  }

  object Rpc {
    //def toRpc[T <: m.Model](model: T)(implicit rpc: RpcCall[T, _]) = rpc.result
    def test[T <: m.Model](model: T)(implicit rpc: RpcCall[T, _]) = rpc.ctype
  }*/
}






