package io.renku.eventlog.metrics

import cats.MonadError
import ch.datascience.config.ConfigLoader
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration

private class MetricsConfigProvider[Interpretation[_]](
    configuration: Config = ConfigFactory.load()
)(implicit ME:     MonadError[Interpretation, Throwable])
    extends ConfigLoader[Interpretation] {

  def getInterval(): Interpretation[FiniteDuration] =
    find[FiniteDuration]("events-log.metrics-scheduler.interval", configuration)
}
