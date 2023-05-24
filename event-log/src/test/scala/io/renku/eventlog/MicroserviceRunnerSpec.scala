/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import MicroserviceRunnerSpec._
import cats.Show
import cats.data.Kleisli
import cats.effect._
import io.circe.{Decoder, Encoder, Json}
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.eventlog.api.events.StatusChangeEvent
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEventsQueue
import io.renku.eventlog.events.producers.{EventProducerStatus, EventProducersRegistry}
import io.renku.eventlog.init.DbInitializer
import io.renku.eventlog.metrics.EventLogMetrics
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.server.HttpServer
import io.renku.interpreters.TestLogger
import io.renku.metrics.GaugeResetScheduler
import io.renku.microservices.{AbstractMicroserviceRunnerTest, CallCounter, ServiceReadinessChecker, ServiceRunCounter}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.Session

import scala.concurrent.duration._

class MicroserviceRunnerSpec extends AnyWordSpec with IOSpec with should.Matchers {

  "run" should {

    "return Success Exit Code " +
      "if Sentry, Events Distributor and metrics initialisation are fine " +
      "and http server starts up" in new TestCase {

        startFor(1.second).unsafeRunSync()
        assertCalledAll.unsafeRunSync()
      }

    "fail if Certificate loading fails" in new TestCase {

      val exception = exceptions.generateOne
      certificateLoader.failWith(exception).unsafeRunSync()

      intercept[Exception](startAndStopRunner.unsafeRunSync()) shouldBe exception
    }

    "fail if Sentry initialisation fails" in new TestCase {

      val exception = exceptions.generateOne
      sentryInitializer.failWith(exception).unsafeRunSync()

      intercept[Exception](startRunnerForever.unsafeRunSync()) shouldBe exception
    }

    "return Success ExitCode even if DB initialisation fails" in new TestCase {

      val exception = exceptions.generateOne
      dbInitializer.failWith(exception).unsafeRunSync()

      startAndStopRunner.unsafeRunSync() shouldBe ExitCode.Success
    }

    "fail if starting the http server fails" in new TestCase {
      val exception = exceptions.generateOne
      httpServer.failWith(exception).unsafeRunSync()

      intercept[Exception](startRunnerForever.unsafeRunSync()) shouldBe exception
    }

    "return Success ExitCode even if Event Producers Registry initialisation fails" in new TestCase {

      eventProducersRegistry.failWith(exceptions.generateOne).unsafeRunSync()

      startFor(1.second).unsafeRunSync() shouldBe ExitCode.Success

      assertCalledAllBut(eventProducersRegistry).unsafeRunSync()
    }

    "return Success ExitCode even if Event Consumers Registry initialisation fails" in new TestCase {

      eventConsumersRegistry.failWith(exceptions.generateOne).unsafeRunSync()

      startFor(1.second).unsafeRunSync() shouldBe ExitCode.Success
      assertCalledAllBut(eventConsumersRegistry).unsafeRunSync()
    }

    "return Success ExitCode even if Event Log Metrics initialisation fails" in new TestCase {

      metrics.failWith(exceptions.generateOne).unsafeRunSync()
      startFor(1.second).unsafeRunSync() shouldBe ExitCode.Success
      assertCalledAllBut(metrics).unsafeRunSync()
    }

    "return Success ExitCode even if StatusChangeEventQueue initialisation fails" in new TestCase {

      eventsQueue.failWith(exceptions.generateOne).unsafeRunSync()
      startFor(1.second).unsafeRunSync() shouldBe ExitCode.Success
      assertCalledAllBut(eventsQueue).unsafeRunSync()
    }

    "return Success ExitCode even if Event Log Metrics scheduler initialisation fails" in new TestCase {
      gaugeScheduler.failWith(exceptions.generateOne).unsafeRunSync()
      startFor(1.second).unsafeRunSync() shouldBe ExitCode.Success
      assertCalledAllBut(gaugeScheduler).unsafeRunSync()
    }
  }
}

object MicroserviceRunnerSpec {
  private trait TestCase extends AbstractMicroserviceRunnerTest {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val serviceReadinessChecker: ServiceReadinessChecker[IO] with CallCounter = new CountEffect(
      "ServerReadinessChecker"
    )
    val certificateLoader:      CertificateLoader[IO] with CallCounter       = new CountEffect("CertificateLoader")
    val sentryInitializer:      SentryInitializer[IO] with CallCounter       = new CountEffect("SentryInitializer")
    val dbInitializer:          DbInitializer[IO] with CallCounter           = new CountEffect("DbInitializer")
    val eventProducersRegistry: EventProducersRegistry[IO] with CallCounter  = new CountEffect("EventProducerRegistry")
    val eventConsumersRegistry: EventConsumersRegistry[IO] with CallCounter  = new CountEffect("EventConsumerRegistry")
    val metrics:                EventLogMetrics[IO] with CallCounter         = new CountEffect("EventLogMetrics")
    val eventsQueue:            StatusChangeEventsQueue[IO] with CallCounter = new CountEffect("StatusChangeEventQueue")
    val gaugeScheduler:         GaugeResetScheduler[IO] with CallCounter     = new CountEffect("GaugeResetScheduler")
    val httpServer:             HttpServer[IO] with CallCounter              = new CountEffect("HttpServer")

    lazy val runner = new MicroserviceRunner(
      serviceReadinessChecker,
      certificateLoader,
      sentryInitializer,
      dbInitializer,
      metrics,
      eventsQueue,
      eventProducersRegistry,
      eventConsumersRegistry,
      gaugeScheduler,
      httpServer
    )

    lazy val all: List[CallCounter] = List(
      serviceReadinessChecker,
      certificateLoader,
      sentryInitializer,
      dbInitializer,
      eventsQueue,
      eventConsumersRegistry,
      eventProducersRegistry,
      metrics,
      gaugeScheduler,
      httpServer
    )
  }

  final class CountEffect(name: String)
      extends ServiceRunCounter(name)
      with DbInitializer[IO]
      with EventLogMetrics[IO]
      with EventProducersRegistry[IO]
      with StatusChangeEventsQueue[IO] {

    override def register(subscriptionRequest: Json): IO[EventProducersRegistry.SubscriptionResult] = ???

    override def getStatus: IO[Set[EventProducerStatus]] = ???

    override def register[E <: StatusChangeEvent](
        eventType: StatusChangeEventsQueue.EventType[E],
        handler:   E => IO[Unit]
    )(implicit decoder: Decoder[E]): IO[Unit] = ???

    override def offer[E <: StatusChangeEvent](event: E)(implicit
        encoder:   Encoder[E],
        eventType: StatusChangeEventsQueue.EventType[E],
        show:      Show[E]
    ): Kleisli[IO, Session[IO], Unit] = ???
  }
}
