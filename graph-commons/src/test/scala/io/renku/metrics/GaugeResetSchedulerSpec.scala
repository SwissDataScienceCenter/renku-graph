/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.metrics

import cats.effect.IO
import cats.syntax.all._
import io.prometheus.client.{Gauge => LibGauge}
import io.renku.config.MetricsConfigProvider
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.lang.Thread.sleep
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

class GaugeResetSchedulerSpec
    extends AnyWordSpec
    with IOSpec
    with Eventually
    with MockFactory
    with IntegrationPatience
    with should.Matchers {

  "run" should {

    "kick off the gauge synchronization process " +
      "that continues endlessly with interval periods" in new TestCase {

        val gaugeScheduler = newGaugeScheduler(refreshing = gauge1, gauge2)

        gauge1.givenResetMethodToReturn add IO.unit
        gauge2.givenResetMethodToReturn add IO.unit

        gaugeScheduler.run().start.unsafeRunAndForget()

        sleep(2 * interval.toMillis + 1000)

        gauge1.resetCallsCount.get() should be > 2
        gauge2.resetCallsCount.get() should be > 2

        logger.expectNoLogs()
      }

    "log an error and continue refreshing in case of failures" in new TestCase {

      val gaugeScheduler = newGaugeScheduler(refreshing = gauge1, gauge2)

      val exception1 = exceptions.generateOne
      gauge1.givenResetMethodToReturn add IO.raiseError(exception1)

      val exception2 = exceptions.generateOne
      gauge2.givenResetMethodToReturn add IO.raiseError(exception2)

      gaugeScheduler.run().start.unsafeRunAndForget()

      sleep(3 * interval.toMillis + 1000)

      gauge1.resetCallsCount.get() should be > 2
      gauge2.resetCallsCount.get() should be > 2

      eventually {
        logger.loggedOnly(Error(s"Clearing event gauge metrics failed", exception1),
                          Error(s"Clearing event gauge metrics failed", exception2)
        )
      }
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gauge1                = new TestLabeledGauge
    val gauge2                = new TestLabeledGauge
    val interval              = 100 millis
    val metricsConfigProvider = mock[MetricsConfigProvider[IO]]

    def newGaugeScheduler(refreshing: LabeledGauge[IO, Double]*) =
      new GaugeResetSchedulerImpl[IO, Double](refreshing.toList, metricsConfigProvider)

    (metricsConfigProvider.getInterval _).expects().returning(interval.pure[IO]).once()

    class TestLabeledGauge extends LabeledGauge[IO, Double] with PrometheusCollector {

      override val name: String = "gauge"
      override val help: String = "help"

      type Collector = LibGauge
      override lazy val wrappedCollector: LibGauge = LibGauge
        .build()
        .name(name)
        .create()

      val givenResetMethodToReturn = new ConcurrentLinkedQueue[IO[Unit]]()
      val resetCallsCount          = new AtomicInteger(0)

      override def reset(): IO[Unit] = {
        resetCallsCount.incrementAndGet()
        Option(givenResetMethodToReturn.poll()) getOrElse IO.unit
      }

      override def clear(): IO[Unit] = fail("Spec shouldn't be calling that")

      override def set(labelValue: (Double, Double)): IO[Unit] =
        fail("Spec shouldn't be calling that")

      override def update(labelValue: (Double, Double)): IO[Unit] =
        fail("Spec shouldn't be calling that")

      override def increment(labelValue: Double): IO[Unit] =
        fail("Spec shouldn't be calling that")

      override def decrement(labelValue: Double): IO[Unit] =
        fail("Spec shouldn't be calling that")
    }
  }
}
