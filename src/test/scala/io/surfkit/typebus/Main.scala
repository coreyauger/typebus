package io.surfkit.typebus

/**
  * Created by suroot on 22/12/16.
  */
object Main extends App{
  case class Number(x: Int) extends m.Model

  trait MyApi{
    def add(x: Number, y: Number): Number
  }

  object MyService extends io.surfkit.typebus.module.Service[MyApi] with MyApi{

    def add(x: Number, y: Number) = Number(x.x+y.x)

    //startService()
  }
  //object
}
