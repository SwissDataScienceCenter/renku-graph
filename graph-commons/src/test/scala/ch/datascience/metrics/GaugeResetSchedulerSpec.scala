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
import cats.effect.{ContextShift, IO}
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
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
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

        val gaugeScheduler = newGaugeScheduler(refreshing = gauge1, gauge2)

        (gauge1.reset _).expects().returning(context.unit).atLeastTwice()
        (gauge2.reset _).expects().returning(context.unit).atLeastTwice()

        gaugeScheduler.run().start.unsafeRunAsyncAndForget()

        sleep(2 * interval.toMillis + 1000)

        logger.expectNoLogs()
      }

    "log an error and continue refreshing in case of failures" in new TestCase {

      val gaugeScheduler = newGaugeScheduler(refreshing = gauge1)

      val exception1 = exceptions.generateOne
      (gauge1.reset _).expects().returning(context.raiseError(exception1))
      val exception2 = exceptions.generateOne
      (gauge1.reset _).expects().returning(context.raiseError(exception2))
      (gauge1.reset _).expects().returning(context.unit).atLeastTwice()

      gaugeScheduler.run().start.unsafeRunAsyncAndForget()

      sleep(3 * interval.toMillis + 1000)

      eventually {
        logger.loggedOnly(Error(s"Clearing event gauge metrics failed", exception1),
                          Error(s"Clearing event gauge metrics failed", exception2)
        )
      }
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private trait TestCase {
    val logger                = TestLogger[IO]()
    val timer                 = IO.timer(global)
    val context               = MonadError[IO, Throwable]
    val gauge1                = mock[LabeledGauge[IO, Double]]
    val gauge2                = mock[LabeledGauge[IO, Double]]
    val interval              = 100 millis
    val metricsConfigProvider = mock[MetricsConfigProvider[IO]]

    def newGaugeScheduler(refreshing: LabeledGauge[IO, Double]*) =
      new GaugeResetSchedulerImpl[IO, Double](refreshing.toList, metricsConfigProvider, logger)(context, timer)

    (metricsConfigProvider.getInterval _).expects().returning(interval.pure[IO]).once()
  }
}
