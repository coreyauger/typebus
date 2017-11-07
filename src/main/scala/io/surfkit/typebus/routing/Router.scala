package io.surfkit.typebus.routing

import akka.actor.ActorRef

import scala.reflect.ClassTag

/**
  * Created by suroot on 27/04/17.
  */
trait Router {

  var listOfRoutes = List.empty[PartialFunction[_, Boolean]]

  def addRouteRule[T <: m.Model : ClassTag](dst: ActorRef*)(filter: (T) => Boolean)(trans: (T) => Any = (t:T) => t ) = {
    def ruleFun = (msg: T) => {
      if(filter(msg)) {
        dst.foreach(_ ! trans(msg))
        true
      }
      else false
    }
    listOfRoutes = funToPF[T]( ruleFun ) :: listOfRoutes
  }

  def route[T <: m.Model : ClassTag](msg: T) = listOfRoutes.map{ f =>
    if(f.asInstanceOf[PartialFunction[m.Model,String]].isDefinedAt(msg))
      f.asInstanceOf[PartialFunction[T, String]](msg)
  }


  protected[this] def funToPF[T <: m.Model : ClassTag](f: (T) => Boolean) = new PartialFunction[T, Boolean] {
    def apply(x: T) = f(x.asInstanceOf[T])
    def isDefinedAt(x: T ) = x match{
      case _: T => true
      case _ => false
    }
  }

}
