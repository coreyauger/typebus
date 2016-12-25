package io.surfkit.typebus.module

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * Created by suroot on 21/12/16.
  */
trait Module {

  var listOfPartials = List.empty[PartialFunction[_, Future[m.Model]]]
  var orchestration: (m.Model) => List[m.Model] = null
  var listOfTopics = List.empty[String]

  protected[this] def op[T <: m.Model : ClassTag](p: PartialFunction[T, Future[m.Model]]) = {
    listOfTopics = scala.reflect.classTag[T].runtimeClass.getCanonicalName.replaceAll("\\$", "") :: listOfTopics
    listOfPartials = p :: listOfPartials
    Unit
  }

  protected[this] def composer(x: Future[m.Model]): Option[Future[m.Model]] = Some(x)

  def orchestrate[T <: m.Model : ClassTag](p: (T) => List[m.Model]) = {
    listOfTopics = scala.reflect.classTag[T].runtimeClass.getCanonicalName.replaceAll("\\$", "") :: listOfTopics
    orchestration = p.asInstanceOf[(m.Model) => List[m.Model]]
    Unit
  }

  protected[this] lazy val handleEvent = listOfPartials.asInstanceOf[List[PartialFunction[m.Model, Future[m.Model]]]].foldRight[PartialFunction[m.Model, Future[m.Model]] ](
    new PartialFunction[m.Model, Future[m.Model]] {
      def apply(x: m.Model) = throw new RuntimeException(s"Type not supported ${x.getClass.getName}") // TODO: more details.. what module when ect?
      def isDefinedAt(x: m.Model ) = true
    })( (a, b) => a.orElse(b) )

}
