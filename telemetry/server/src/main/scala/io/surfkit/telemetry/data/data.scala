package io.surfkit.telemetry

import java.util.UUID
import io.surfkit.typebus._
import org.joda.time.{DateTime, LocalDate}

package object data {

  trait BaseType

  object Implicits extends AvroByteStreams{
  }

}






