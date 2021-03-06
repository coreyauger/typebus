package io.surfkit.typebus.module

import io.surfkit.typebus.bus.StreamBuilder
import io.surfkit.typebus.{ByteStreamReader, ByteStreamWriter}
import io.surfkit.typebus.event._

import scala.concurrent.Future
import scala.reflect.ClassTag

/***
  * Module is the base of ALL typebus service.
  */
trait Module {
  import scala.reflect.runtime.universe._

  var listOfPartialsWithMeta = List.empty[PartialFunction[_, Future[Any]]]
  var listOfPartialsWithMetaUnit = List.empty[PartialFunction[_, Future[Unit]]]
  var listOfImplicitsReaders = Map.empty[EventType, ByteStreamReader[Any] ]
  var listOfImplicitsWriters = Map.empty[EventType, ByteStreamWriter[Any] ]
  var listOfFunctions = Map.empty[EventType, EventType]

  var listOfServicePartialsWithMeta = List.empty[PartialFunction[_, Future[TypeBus]]]
  var listOfServiceImplicitsReaders = Map.empty[EventType, ByteStreamReader[TypeBus] ]
  var listOfServiceImplicitsWriters = Map.empty[EventType, ByteStreamWriter[TypeBus] ]
  var listOfServiceFunctions = Map.empty[EventType, EventType]

  var streamBuilderMap = Map.empty[EventType, StreamBuilder[_, _]]

  def paramInfo[T: TypeTag]: List[String] = {
      val targs = typeOf[T] match { case TypeRef(_, _, args) => args }
      targs.map(_.typeSymbol.fullName)
   }

  def fullQualifiedEventType[T : ClassTag : TypeTag]: EventType = {
    val inTypeParams =  paramInfo[T]
    val itp = inTypeParams match{
      case Nil => ""
      case xs => xs.mkString("[",",","]")
    }
    EventType.parse(scala.reflect.classTag[T].runtimeClass.getCanonicalName + itp)
  }

  /***
    * op2 - internal method to register a partial function that also receives EventMeta
    * @param p - the partial function to register.
    * @param reader - ByteStreamReader that knows how to convert Array[Byte] to a type T
    * @param writer - ByteStreamWriter that knows how to convert a type U to Array[Byte]
    * @tparam T - The IN service request type
    * @tparam U - The OUT service request type
    * @return - Unit
    */
  protected[this] def op2[T <: Any : ClassTag : TypeTag, U <: Any : ClassTag : TypeTag](p: PartialFunction[(T, EventMeta), Future[U]])(implicit reader: ByteStreamReader[T], writer: ByteStreamWriter[U] ): StreamBuilder[T, U] = {
    val topic = fullQualifiedEventType[T]
    val returnType = fullQualifiedEventType[U]  // will construct a type and include type params
    listOfFunctions += (topic -> returnType)
    listOfPartialsWithMeta = p :: listOfPartialsWithMeta
    listOfImplicitsReaders +=  (topic -> reader.asInstanceOf[ByteStreamReader[Any]])
    listOfImplicitsWriters +=  (returnType -> writer.asInstanceOf[ByteStreamWriter[Any]])
    val sb = new StreamBuilder[T, U] {}
    streamBuilderMap += topic -> sb
    streamBuilderMap += returnType -> sb
    sb
  }

  /***
    * op2Unit - register a sink
    * @param p - the partial function to register.
    * @param reader - ByteStreamReader that knows how to convert Array[Byte] to a type T
    * @tparam T - The IN service request type
    * @return - Unit
    */
  protected[this] def op2Unit[T <: Any : ClassTag : TypeTag](p: PartialFunction[(T, EventMeta), Future[Unit] ])(implicit reader: ByteStreamReader[T] ): StreamBuilder[T, Unit]  = {
    val topic = fullQualifiedEventType[T]
    listOfFunctions += (topic -> EventType.parse("scala.Unit"))
    listOfPartialsWithMetaUnit = p :: listOfPartialsWithMetaUnit
    listOfImplicitsReaders +=  (topic -> reader.asInstanceOf[ByteStreamReader[Any]])
    val sb = new StreamBuilder[T, Unit] {}
    streamBuilderMap += topic -> sb
    sb
  }

  /***
    * op2Service - internal method to register HIDDEN TypeBus Service level functions
    * @param p - the partial function to register.
    * @param reader - ByteStreamReader that knows how to convert Array[Byte] to a type T
    * @param writer - ByteStreamWriter that knows how to convert a type U to Array[Byte]
    * @tparam T - The IN service request type
    * @tparam U - The OUT service request type
    * @return - Unit
    */
  protected[this] def op2Service[T <: TypeBus : ClassTag : TypeTag, U <: TypeBus : ClassTag: TypeTag](p: PartialFunction[(T, EventMeta), Future[U]])(implicit reader: ByteStreamReader[T], writer: ByteStreamWriter[U] ): StreamBuilder[T, U] = {
    val topic = fullQualifiedEventType[T]
    val returnType = fullQualifiedEventType[U]  // will construct a type and include type params
    listOfServiceFunctions += (topic -> returnType)
    listOfServicePartialsWithMeta = p :: listOfServicePartialsWithMeta
    listOfServiceImplicitsReaders +=  (topic -> reader.asInstanceOf[ByteStreamReader[TypeBus]])
    listOfServiceImplicitsWriters +=  (returnType -> writer.asInstanceOf[ByteStreamWriter[TypeBus]])
    val sb = new StreamBuilder[T, U] {}
    streamBuilderMap += topic -> sb
    streamBuilderMap += returnType -> sb
    sb
  }

  /***
    * funToPF - convert a function to a partial function
    * @param f - the function to convert
    * @tparam T - The IN service request type
    * @tparam S - The OUT service request type
    * @return - PartialFunction[T, Future[S]]
    */
  protected[this] def funToPF[T : ClassTag, S : ClassTag](f: (T) => Future[S]) = new PartialFunction[T, Future[S]] {
    def apply(x: T) = f(x.asInstanceOf[T])
    def isDefinedAt(x: T ) = x match{
      case _: T => true
      case _ => false
    }
  }

  /***
    * funToPF2 - convert a function to a partial function with EventMeta
    * @param f - the function to convert
    * @tparam T - The IN service request type
    * @tparam S - The OUT service request type
    * @return - PartialFunction[T, Future[S]]
    */
  protected[this] def funToPF2[T : ClassTag, S : ClassTag](f: (T, EventMeta) => Future[S]) = new PartialFunction[ (T,EventMeta), Future[S]] {
    def apply(x: (T, EventMeta) ) = f(x._1.asInstanceOf[T], x._2)
    def isDefinedAt(x: (T, EventMeta) ) = x._1 match{
      case _: T => true
      case _ => false
    }
  }

  /***
    * funToPF2Unit - convert a function to a partial function sink
    * @param f - the function to convert
    * @tparam T - The IN service request type
    * @tparam - Future[Unit] sink
    * @return - PartialFunction[ (T,EventMeta), Future[Unit]]
    */
  protected[typebus] def funToPF2Unit[T : ClassTag, Future[Unit]](f: (T, EventMeta) => Future[Unit]) = new PartialFunction[ (T,EventMeta), Future[Unit]] {
    def apply(x: (T, EventMeta) ) = f(x._1.asInstanceOf[T], x._2)
    def isDefinedAt(x: (T, EventMeta) ) = x._1 match{
      case _: T => true
      case _ => false
    }
  }

  protected[typebus] lazy val handleEventWithMeta = listOfPartialsWithMeta.asInstanceOf[List[PartialFunction[ (Any, EventMeta), Future[Any]]]].foldRight[PartialFunction[ (Any, EventMeta), Future[Any]] ](
    new PartialFunction[ (Any, EventMeta), Future[Any]] {
      def apply(x: (Any, EventMeta)) = throw new RuntimeException(s"Type not supported ${x._1.getClass.getCanonicalName}") // TODO: This needs to fail when we don't have the implicit
      def isDefinedAt(x: (Any, EventMeta) ) = false
    })( (a, b) => a.orElse(b) )

  protected[typebus] lazy val handleEventWithMetaUnit = listOfPartialsWithMetaUnit.asInstanceOf[List[PartialFunction[ (Any, EventMeta), Future[Unit]]]].foldRight[PartialFunction[ (Any, EventMeta), Future[Unit]] ](
    new PartialFunction[ (Any, EventMeta), Future[Unit]] {
      def apply(x: (Any, EventMeta)) = throw new RuntimeException(s"Type not supported ${x._1.getClass.getCanonicalName}") // TODO: This needs to fail when we don't have the implicit
      def isDefinedAt(x: (Any, EventMeta) ) = false
    })( (a, b) => a.orElse(b) )

  protected[typebus] lazy val handleServiceEventWithMeta = listOfServicePartialsWithMeta.asInstanceOf[List[PartialFunction[ (Any, EventMeta), Future[Any]]]].foldRight[PartialFunction[ (Any, EventMeta), Future[Any]] ](
    new PartialFunction[ (Any, EventMeta), Future[Any]] {
      def apply(x: (Any, EventMeta)) = throw new RuntimeException(s"Type not supported ${x._1.getClass.getCanonicalName}") // TODO: This needs to fail when we don't have the implicit
      def isDefinedAt(x: (Any, EventMeta) ) = false
    })( (a, b) => a.orElse(b) )


}
