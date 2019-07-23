package ch.datascience.graph.acceptancetests.testing

import org.scalatest.concurrent.AbstractPatienceConfiguration
import org.scalatest.time.{Millis, Seconds, Span}

trait AcceptanceTestPatience extends AbstractPatienceConfiguration {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout  = scaled(Span(30, Seconds)),
    interval = scaled(Span(150, Millis))
  )
}
