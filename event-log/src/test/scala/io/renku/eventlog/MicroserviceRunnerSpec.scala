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

package io.renku.eventlog

import java.util.concurrent.ConcurrentHashMap

import cats.MonadError
import cats.effect._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.server.IOHttpServer
import ch.datascience.interpreters.IOSentryInitializer
import ch.datascience.metrics.{LabeledGauge, SingleValueGauge}
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.metrics.{EventGaugeScheduler, EventLogMetrics, StatsFinder}
import io.renku.eventlog.subscriptions.EventsDispatcher
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

class MicroserviceRunnerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {

    "return Success Exit Code " +
      "if Sentry, Events Dispatcher and metrics initialisation are fine " +
      "and http server starts up" in new TestCase {

        (sentryInitializer.run _)
          .expects()
          .returning(IO.unit)

        (metrics.run _)
          .expects()
          .returning(IO.unit)

        (eventsDispatcher.run _)
          .expects()
          .returning(IO.unit)

        (gaugeScheduler.run _)
          .expects()
          .returning(IO.unit)

        (httpServer.run _)
          .expects()
          .returning(context.pure(ExitCode.Success))

        runner.run().unsafeRunSync() shouldBe ExitCode.Success
      }

    "fail if Sentry initialisation fails" in new TestCase {

      val exception = exceptions.generateOne
      (sentryInitializer.run _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if starting the http server fails" in new TestCase {

      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (metrics.run _)
        .expects()
        .returning(IO.unit)

      (eventsDispatcher.run _)
        .expects()
        .returning(IO.unit)

      (gaugeScheduler.run _)
        .expects()
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (httpServer.run _)
        .expects()
        .returning(context.raiseError(exception))

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "return Success ExitCode even if Events Dispatcher initialisation fails" in new TestCase {
      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (eventsDispatcher.run _)
        .expects()
        .returning(IO.raiseError(exception))

      (metrics.run _)
        .expects()
        .returning(IO.unit)

      (gaugeScheduler.run _)
        .expects()
        .returning(IO.unit)

      (httpServer.run _)
        .expects()
        .returning(context.pure(ExitCode.Success))

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }

    "return Success ExitCode even if Event Log Metrics initialisation fails" in new TestCase {
      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventsDispatcher.run _)
        .expects()
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (metrics.run _)
        .expects()
        .returning(IO.raiseError(exception))

      (gaugeScheduler.run _)
        .expects()
        .returning(IO.unit)

      (httpServer.run _)
        .expects()
        .returning(context.pure(ExitCode.Success))

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }

    "return Success ExitCode even if Event Log Metrics scheduler initialisation fails" in new TestCase {
      (sentryInitializer.run _)
        .expects()
        .returning(IO.unit)

      (eventsDispatcher.run _)
        .expects()
        .returning(IO.unit)

      (metrics.run _)
        .expects()
        .returning(IO.unit)

      val exception = exceptions.generateOne
      (gaugeScheduler.run _)
        .expects()
        .returning(IO.raiseError(exception))

      (httpServer.run _)
        .expects()
        .returning(context.pure(ExitCode.Success))

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val sentryInitializer = mock[IOSentryInitializer]
    val eventsDispatcher  = mock[EventsDispatcher]
    val metrics           = mock[TestEventLogMetrics]
    val httpServer        = mock[IOHttpServer]
    val gaugeScheduler    = mock[EventGaugeScheduler[IO]]
    val runner = new MicroserviceRunner(sentryInitializer,
                                        metrics,
                                        eventsDispatcher,
                                        gaugeScheduler,
                                        httpServer,
                                        new ConcurrentHashMap[CancelToken[IO], Unit]()
    )

    class TestEventLogMetrics(
        statsFinder:      StatsFinder[IO],
        logger:           Logger[IO],
        statusesGauge:    LabeledGauge[IO, EventStatus],
        totalGauge:       SingleValueGauge[IO],
        interval:         FiniteDuration,
        statusesInterval: FiniteDuration
    )(implicit ME:        MonadError[IO, Throwable], timer: Timer[IO], cs: ContextShift[IO])
        extends EventLogMetrics(
          statsFinder,
          logger,
          statusesGauge,
          totalGauge,
          interval,
          statusesInterval
        )
  }
}
