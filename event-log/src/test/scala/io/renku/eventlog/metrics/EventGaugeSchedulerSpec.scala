package io.renku.eventlog.metrics

import java.lang.Thread.sleep

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.interpreters.TestLogger
import ch.datascience.metrics.LabeledGauge
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

class EventGaugeSchedulerSpec extends AnyWordSpec with MockFactory with should.Matchers {
  "kick off the gauge synchronization process and " +
    "continues endlessly with interval periods" in new TestCase {
      (metricsConfigProvider.getInterval _).expects().returning(interval.pure[IO]).atLeastOnce()
      (timer
        .sleep(_: FiniteDuration))
        .expects(interval)
        .returning(context.unit)
        .atLeastOnce()
      (gauge.reset _).expects().returning(context.unit)
      gaugeScheduler.run().unsafeRunAsyncAndForget()

      sleep(interval.toMillis + 1000)
      logger.expectNoLogs()
    }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private trait TestCase {
    val logger                = TestLogger[IO]()
    val context               = MonadError[IO, Throwable]
    val gauge                 = mock[LabeledGauge[IO, Double]]
    val timer                 = mock[Timer[IO]]
    val interval              = 5 seconds
    val metricsConfigProvider = mock[MetricsConfigProvider[IO]]
    val gaugeScheduler =
      new EventGaugeSchedulerImpl[IO, Double](List(gauge), metricsConfigProvider, logger)(context, timer)
  }
}
