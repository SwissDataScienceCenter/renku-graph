package io.renku.eventlog.metrics

import cats.MonadError
import cats.effect.{IO, Timer}
import cats.syntax.all._
import ch.datascience.metrics.LabeledGauge
import io.chrisdavenport.log4cats.Logger

trait EventGaugeScheduler[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

class EventGaugeSchedulerImpl[Interpretation[_], LabelValue](
    gauges:                 List[LabeledGauge[Interpretation, LabelValue]],
    metricsSchedulerConfig: MetricsConfigProvider[Interpretation],
    logger:                 Logger[Interpretation]
)(implicit ME:              MonadError[Interpretation, Throwable], timer: Timer[Interpretation])
    extends EventGaugeScheduler[Interpretation] {
  override def run(): Interpretation[Unit] = (for {
    interval <- metricsSchedulerConfig.getInterval()
    _        <- timer sleep interval
    _        <- gauges.map(_.reset()).sequence
  } yield ()) recoverWith logError

  private lazy val logError: PartialFunction[Throwable, Interpretation[Unit]] = { case e: Throwable =>
    logger.error(s"Clearing event gauge metrics failed with - ${e.getMessage}")
  }
}

object IOEventGaugeScheduler {
  def apply[LabelValue](
      gauges:    List[LabeledGauge[IO, LabelValue]],
      logger:    Logger[IO]
  )(implicit ME: MonadError[IO, Throwable], timer: Timer[IO]): IO[EventGaugeSchedulerImpl[IO, LabelValue]] = IO(
    new EventGaugeSchedulerImpl[IO, LabelValue](gauges, new MetricsConfigProviderImpl[IO](), logger)
  )
}
