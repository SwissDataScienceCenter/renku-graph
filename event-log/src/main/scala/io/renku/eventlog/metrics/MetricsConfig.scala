package io.renku.eventlog.metrics

import cats.MonadError
import ch.datascience.config.ConfigLoader
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration

trait MetricsConfigProvider[Interpretation[_]] extends ConfigLoader[Interpretation] {
  def getInterval(): Interpretation[FiniteDuration]
}

class MetricsConfigProviderImpl[Interpretation[_]](
    configuration: Config = ConfigFactory.load()
)(implicit ME:     MonadError[Interpretation, Throwable])
    extends MetricsConfigProvider[Interpretation] {
  def getInterval(): Interpretation[FiniteDuration] =
    find[FiniteDuration]("event-log.metrics.scheduler-reset-interval", configuration)
}
