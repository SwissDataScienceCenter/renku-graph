package io.renku.eventlog.metrics

import cats.MonadError
import cats.effect.{IO, Timer}
import ch.datascience.metrics.LabeledGauge
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

class EventGaugeSchedulerSpec
    extends AnyWordSpec
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {
  "kick off the gauge synchronization process and " +
    "continues endlessly with interval periods" in new TestCase {

      (timer
        .sleep(_: FiniteDuration))
        .expects(5 minutes)
        .returning(context.unit)

      IO.suspend(gaugeScheduler.run()).unsafeRunAsyncAndForget()

      eventually {
        (gaugeScheduler.run _).expects().returning(context.unit)
      }
    }

  private trait TestCase {
    val context               = MonadError[IO, Throwable]
    val gauge                 = mock[LabeledGauge[IO, Double]]
    val timer                 = mock[Timer[IO]]
    val metricsConfigProvider = mock[MetricsConfigProvider[IO]]
    val gaugeScheduler        = new EventGaugeSchedulerImpl[IO, Double](List(gauge), metricsConfigProvider)(context, timer)
  }
}
