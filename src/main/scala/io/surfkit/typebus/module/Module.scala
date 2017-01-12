package io.surfkit.typebus.module

import io.surfkit.typebus.event.PublishedEvent

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * Created by suroot on 21/12/16.
  */
trait Module {

  var listOfPartials = List.empty[PartialFunction[_, Future[m.Model]]]
  //var orchestration = List.empty[PartialFunction[PublishedEvent[m.Model], Future[m.Model]]]
  var orchestrationMap = Map.empty[String, (PublishedEvent[m.Model]) => Future[m.Model]]
  var listOfTopics = List.empty[String]

  protected[this] def op[T <: m.Model : ClassTag](p: PartialFunction[T, Future[m.Model]]) = {
    listOfTopics = scala.reflect.classTag[T].runtimeClass.getCanonicalName.replaceAll("\\$", "") :: listOfTopics
    listOfPartials = p :: listOfPartials
    println(s"partial: ${p} ${scala.reflect.classTag[T].runtimeClass.getCanonicalName.replaceAll("\\$", "")}")
    Unit
  }

  protected[this] def funToPF[T <: m.Model : ClassTag](f: (T) => Future[m.Model]) = new PartialFunction[T, Future[m.Model]] {
    def apply(x: T) = f(x.asInstanceOf[T])
    def isDefinedAt(x: T ) = x match{
      case _: T => true
      case _ => false
    }
  }

  protected[this] def funToPEF[T <: m.Model : ClassTag](f: (PublishedEvent[T]) => Future[m.Model]) = new PartialFunction[PublishedEvent[T], Future[m.Model]] {
    def apply(x: PublishedEvent[T]) = f(x)
    def isDefinedAt(x: PublishedEvent[T]) = x match{
      case _: T => true
      case _ => false
    }
  }

  protected[this] def orchestrate[T <: m.Model : ClassTag](p: (PublishedEvent[T]) => Future[m.Model]) = {
    val topic = scala.reflect.classTag[T].runtimeClass.getCanonicalName.replaceAll("\\$", "")
    listOfTopics = topic :: listOfTopics
    //orchestration = p.asInstanceOf[PartialFunction[PublishedEvent[m.Model], Future[m.Model]]] :: orchestration
    orchestrationMap += topic -> p.asInstanceOf[ (PublishedEvent[m.Model]) => Future[m.Model] ]
    println(s"partial: ${p} ${topic}")
    Unit
  }

  /*protected[this] lazy val handleOrchestrate = orchestration.foldRight[PartialFunction[PublishedEvent[m.Model], Future[m.Model]] ](
    new PartialFunction[PublishedEvent[m.Model], Future[m.Model]] {
      def apply(x: PublishedEvent[m.Model]) = throw new RuntimeException(s"Type not supported ${x.getClass.getName}") // TODO: more details.. what module when ect?
      def isDefinedAt(x: PublishedEvent[m.Model] ) = true
    })( (a, b) => a.orElse(b) )*/

  protected[this] def handleOrchestrate(event: PublishedEvent[m.Model]) = {
    val topic = event.payload.getClass.getCanonicalName.replaceAll("\\$", "")
    orchestrationMap(topic)(event)
  }

  protected[this] lazy val handleEvent = listOfPartials.asInstanceOf[List[PartialFunction[m.Model, Future[m.Model]]]].foldRight[PartialFunction[m.Model, Future[m.Model]] ](
    new PartialFunction[m.Model, Future[m.Model]] {
      def apply(x: m.Model) = throw new RuntimeException(s"Type not supported ${x.getClass.getName}") // TODO: more details.. what module when ect?
      def isDefinedAt(x: m.Model ) = true
    })( (a, b) => a.orElse(b) )

}
