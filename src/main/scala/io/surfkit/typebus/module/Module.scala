package io.surfkit.typebus.module

import io.surfkit.typebus.{ByteStreamReader, ByteStreamWriter}
import io.surfkit.typebus.event.PublishedEvent

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * Created by suroot on 21/12/16.
  */
trait Module {

  var listOfPartials = List.empty[PartialFunction[_, Future[AnyVal]]]
  var listOfImplicits = Map.empty[String, ByteStreamReader[PublishedEvent[AnyVal]] ]
  //var orchestration = List.empty[PartialFunction[PublishedEvent[m.Model], Future[m.Model]]]
  var orchestrationMap = Map.empty[String, (PublishedEvent[AnyVal]) => Future[AnyVal]]
  var listOfTopics = List.empty[String]

  protected[this] def op[T : ClassTag, U <: AnyVal](p: PartialFunction[T, Future[U]])(implicit reader: ByteStreamReader[PublishedEvent[U]] )  = {
    val topic = scala.reflect.classTag[T].runtimeClass.getCanonicalName
    listOfTopics = topic :: listOfTopics
    listOfPartials = p :: listOfPartials
    listOfImplicits +=  (topic -> reader.asInstanceOf[ ByteStreamReader[PublishedEvent[AnyVal]] ])
    println(s"partial: ${p} ${scala.reflect.classTag[T].runtimeClass.getCanonicalName}")
    Unit
  }

  protected[this] def funToPF[T : ClassTag : ByteStreamReader : ByteStreamWriter, S <: AnyVal : ByteStreamReader : ByteStreamWriter](f: (T) => Future[S]) = new PartialFunction[T, Future[S]] {
    def apply(x: T) = f(x.asInstanceOf[T])
    def isDefinedAt(x: T ) = x match{
      case _: T => true
      case _ => false
    }
  }

  /*protected[this] def funToPEF[T : ClassTag](f: (PublishedEvent[T]) => Future[m.Model]) = new PartialFunction[PublishedEvent[T], Future[m.Model]] {
    def apply(x: PublishedEvent[T]) = f(x)
    def isDefinedAt(x: PublishedEvent[T]) = x match{
      case _: T => true
      case _ => false
    }
  }

  protected[this] def orchestrate[T : ClassTag, S <: AnyVal](p: (PublishedEvent[T]) => Future[S]) = {
    val topic = scala.reflect.classTag[T].runtimeClass.getCanonicalName
    listOfTopics = topic :: listOfTopics
    //orchestration = p.asInstanceOf[PartialFunction[PublishedEvent[m.Model], Future[m.Model]]] :: orchestration
    orchestrationMap += topic -> p.asInstanceOf[ (PublishedEvent[AnyVal]) => Future[S] ]
    println(s"partial: ${p} ${topic}")
    Unit
  }


  protected[this] def handleOrchestrate(event: PublishedEvent[AnyVal]) = {
    val topic = event.payload.getClass.getCanonicalName
    orchestrationMap(topic)(event)
  }*/

  protected[this] lazy val handleEvent = listOfPartials.asInstanceOf[List[PartialFunction[AnyVal, Future[AnyVal]]]].foldRight[PartialFunction[AnyVal, Future[AnyVal]] ](
    new PartialFunction[AnyVal, Future[AnyVal]] {
      def apply(x: AnyVal) = throw new RuntimeException(s"Type not supported ${x.getClass.getName}") // TODO: This needs to fail when we don't have the implicit
      def isDefinedAt(x: AnyVal ) = true
    })( (a, b) => a.orElse(b) )

}
