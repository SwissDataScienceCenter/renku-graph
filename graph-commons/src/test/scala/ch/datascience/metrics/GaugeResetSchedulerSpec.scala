/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.metrics

import java.lang.Thread.sleep

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.MetricsConfigProvider
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.language.postfixOps

class GaugeResetSchedulerSpec
    extends AnyWordSpec
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {
  "run" should {

    "kick off the gauge synchronization process and " +
      "continues endlessly with interval periods" in new TestCase {
        val gaugeScheduler =
          new GaugeResetSchedulerImpl[IO, Double](List(gauge1, gauge2), metricsConfigProvider, logger)(context, timer)
        (metricsConfigProvider.getInterval _).expects().returning(interval.pure[IO])
        (timer
          .sleep(_: FiniteDuration))
          .expects(interval)
          .returning(context.unit)
          .atLeastTwice()
        (gauge1.reset _).expects().returning(context.unit).atLeastTwice()
        (gauge2.reset _).expects().returning(context.unit).atLeastTwice()
        gaugeScheduler.run().start.unsafeRunAsyncAndForget()

        sleep(2 * interval.toMillis + 1000)
        logger.expectNoLogs()
      }

    "log an error in case of failure" in new TestCase {

      val gaugeScheduler =
        new GaugeResetSchedulerImpl[IO, Double](List(gauge1), metricsConfigProvider, logger)(context, timer)
      val exception = new Exception(exceptions.generateOne)

      (metricsConfigProvider.getInterval _).expects().returning(interval.pure[IO]).once()
      (timer
        .sleep(_: FiniteDuration))
        .expects(interval)
        .returning(context.unit)
        .atLeastOnce()
      (gauge1.reset _).expects().returning(context.raiseError(exception))
      gaugeScheduler.run().start.unsafeRunAsyncAndForget()

      eventually {
        logger.loggedOnly(Error(s"Clearing event gauge metrics failed with - ${exception.getMessage}"))
      }
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private trait TestCase {
    val logger                = TestLogger[IO]()
    val context               = MonadError[IO, Throwable]
    val gauge1                = mock[LabeledGauge[IO, Double]]
    val gauge2                = mock[LabeledGauge[IO, Double]]
    val timer                 = mock[Timer[IO]]
    val interval              = 1 seconds
    val metricsConfigProvider = mock[MetricsConfigProvider[IO]]

  }
}
