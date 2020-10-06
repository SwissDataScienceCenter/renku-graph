package io.renku.eventlog.metrics

import cats.MonadError
import cats.effect.{IO, Timer}
import cats.syntax.all._
import ch.datascience.metrics.LabeledGauge

trait EventGaugeScheduler[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

class EventGaugeSchedulerImpl[Interpretation[_], LabelValue](
    gauges:                 List[LabeledGauge[Interpretation, LabelValue]],
    metricsSchedulerConfig: MetricsConfigProvider[Interpretation]
)(implicit ME:              MonadError[Interpretation, Throwable], timer: Timer[Interpretation])
    extends EventGaugeScheduler[Interpretation] {
  override def run(): Interpretation[Unit] = for {
    interval <- metricsSchedulerConfig.getInterval()
    _        <- timer sleep interval
    _        <- gauges.map(_.reset()).sequence
  } yield ()
}

object IOEventGaugeScheduler {
  def apply[LabelValue](
      gauges:    List[LabeledGauge[IO, LabelValue]]
  )(implicit ME: MonadError[IO, Throwable], timer: Timer[IO]): IO[EventGaugeSchedulerImpl[IO, LabelValue]] = IO(
    new EventGaugeSchedulerImpl[IO, LabelValue](gauges, new MetricsConfigProvider[IO]())
  )
}
