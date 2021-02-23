/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.effect._
import ch.datascience.config.certificates.CertificateLoader
import ch.datascience.events.consumers.EventConsumersRegistry
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.server.IOHttpServer
import ch.datascience.interpreters.IOSentryInitializer
import ch.datascience.metrics.GaugeResetScheduler
import ch.datascience.testtools.MockedRunnableCollaborators
import io.renku.eventlog.init.DbInitializer
import io.renku.eventlog.metrics.EventLogMetrics
import io.renku.eventlog.subscriptions.EventProducersRegistry
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext.Implicits.global

class MicroserviceRunnerSpec
    extends AnyWordSpec
    with MockedRunnableCollaborators
    with MockFactory
    with should.Matchers {

  "run" should {

    "return Success Exit Code " +
      "if Sentry, Events Distributor and metrics initialisation are fine " +
      "and http server starts up" in new TestCase {

        given(certificateLoader).succeeds(returning = ())
        given(sentryInitializer).succeeds(returning = ())
        given(dbInitializer).succeeds(returning = ())
        given(metrics).succeeds(returning = ())
        given(eventProducersRegistry).succeeds(returning = ())
        given(eventConsumersRegistry).succeeds(returning = ())
        given(gaugeScheduler).succeeds(returning = ())
        given(httpServer).succeeds(returning = ExitCode.Success)

        runner.run().unsafeRunSync() shouldBe ExitCode.Success
      }

    "fail if Certificate loading fails" in new TestCase {

      val exception = exceptions.generateOne
      given(certificateLoader).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if Sentry initialisation fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(sentryInitializer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if db initialisation fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(dbInitializer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if starting the http server fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      given(metrics).succeeds(returning = ())
      given(eventProducersRegistry).succeeds(returning = ())
      given(eventConsumersRegistry).succeeds(returning = ())
      given(gaugeScheduler).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(httpServer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "return Success ExitCode even if Event Producers Registry initialisation fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      given(metrics).succeeds(returning = ())
      given(eventProducersRegistry).fails(becauseOf = exceptions.generateOne)
      given(eventConsumersRegistry).succeeds(returning = ())
      given(gaugeScheduler).succeeds(returning = ())
      given(httpServer).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }

    "return Success ExitCode even if Event Consumers Registry initialisation fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      given(metrics).succeeds(returning = ())
      given(eventProducersRegistry).succeeds(returning = ())
      given(eventConsumersRegistry).fails(becauseOf = exceptions.generateOne)
      given(gaugeScheduler).succeeds(returning = ())
      given(httpServer).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }

    "return Success ExitCode even if Event Log Metrics initialisation fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      given(metrics).fails(becauseOf = exceptions.generateOne)
      given(eventProducersRegistry).succeeds(returning = ())
      given(eventConsumersRegistry).succeeds(returning = ())
      given(gaugeScheduler).succeeds(returning = ())
      given(httpServer).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }

    "return Success ExitCode even if Event Log Metrics scheduler initialisation fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      given(metrics).succeeds(returning = ())
      given(eventProducersRegistry).succeeds(returning = ())
      given(eventConsumersRegistry).succeeds(returning = ())
      given(gaugeScheduler).fails(becauseOf = exceptions.generateOne)
      given(httpServer).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val certificateLoader      = mock[CertificateLoader[IO]]
    val sentryInitializer      = mock[IOSentryInitializer]
    val dbInitializer          = mock[DbInitializer[IO]]
    val eventProducersRegistry = mock[EventProducersRegistry[IO]]
    val eventConsumersRegistry = mock[EventConsumersRegistry[IO]]
    val metrics                = mock[EventLogMetrics[IO]]
    val httpServer             = mock[IOHttpServer]
    val gaugeScheduler         = mock[GaugeResetScheduler[IO]]
    val runner = new MicroserviceRunner(
      certificateLoader,
      sentryInitializer,
      dbInitializer,
      metrics,
      eventProducersRegistry,
      eventConsumersRegistry,
      gaugeScheduler,
      httpServer,
      new ConcurrentHashMap[CancelToken[IO], Unit]()
    )
  }
}
