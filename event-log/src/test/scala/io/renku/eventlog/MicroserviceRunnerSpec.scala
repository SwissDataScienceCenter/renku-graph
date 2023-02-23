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

import cats.Show
import cats.data.Kleisli
import cats.effect._
import cats.syntax.all._
import fs2.concurrent.SignallingRef
import io.circe.{Decoder, Encoder, Json}
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.eventlog.events.consumers.statuschange.{StatusChangeEvent, StatusChangeEventsQueue}
import io.renku.eventlog.events.producers.{EventProducerStatus, EventProducersRegistry}
import io.renku.eventlog.init.DbInitializer
import io.renku.eventlog.metrics.EventLogMetrics
import io.renku.events.EventRequestContent
import io.renku.events.consumers.{EventConsumersRegistry, EventSchedulingResult}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.server.HttpServer
import io.renku.interpreters.TestLogger
import io.renku.metrics.GaugeResetScheduler
import io.renku.microservices.ServiceReadinessChecker
import io.renku.testtools.{IOSpec, MockedRunnableCollaborators}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.Session

import scala.concurrent.duration._

class MicroserviceRunnerSpec
    extends AnyWordSpec
    with IOSpec
    with MockedRunnableCollaborators
    with MockFactory
    with Eventually
    with IntegrationPatience
    with should.Matchers {

  "run" should {

    "return Success Exit Code " +
      "if Sentry, Events Distributor and metrics initialisation are fine " +
      "and http server starts up" in new TestCase {

//        given(certificateLoader).succeeds(returning = ())
//        given(sentryInitializer).succeeds(returning = ())
//        given(dbInitializer).succeeds(returning = ())
//        (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
//        given(eventProducersRegistry).succeeds(returning = ())
//        given(eventConsumersRegistry).succeeds(returning = ())
//        given(eventsQueue).succeeds(returning = ())
//        given(httpServer).succeeds()

        startRunnerFor(0.5.seconds).unsafeRunSync()

        metrics.counter.get.unsafeRunSync()        should be > 1
        gaugeScheduler.counter.get.unsafeRunSync() should be > 1
      }

    "fail if Certificate loading fails" in new TestCase {

      val exception = exceptions.generateOne
      given(certificateLoader).fails(becauseOf = exception)

      intercept[Exception](startRunnerForever.unsafeRunSync()) shouldBe exception
    }

    "fail if Sentry initialisation fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(sentryInitializer).fails(becauseOf = exception)

      intercept[Exception](startRunnerForever.unsafeRunSync()) shouldBe exception
    }

    "return Success ExitCode even if DB initialisation fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(dbInitializer).fails(becauseOf = exception)
      given(httpServer).succeeds()

      startRunnerFor(1.seconds).unsafeRunSync() shouldBe ExitCode.Success
    }

    "fail if starting the http server fails" in new TestCase {
      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      // given(dbInitializer).succeeds(returning = ())
      // (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
      // given(eventProducersRegistry).succeeds(returning = ())
      // given(eventConsumersRegistry).succeeds(returning = ())
      // given(eventsQueue).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(httpServer).fails(becauseOf = exception)

      intercept[Exception](startRunnerForever.unsafeRunSync()) shouldBe exception
    }

    "return Success ExitCode even if Event Producers Registry initialisation fails" in new TestCase {

      override val metrics        = new Metrics()
      override val gaugeScheduler = new Gauge()

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
      given(eventProducersRegistry).fails(becauseOf = exceptions.generateOne)
      given(eventConsumersRegistry).succeeds(returning = ())
      given(eventsQueue).succeeds(returning = ())
      given(httpServer).succeeds()

      startRunnerFor(2.seconds).unsafeRunSync() shouldBe ExitCode.Success

      eventually {
        metrics.counter.get.unsafeRunSync()        should be > 1
        gaugeScheduler.counter.get.unsafeRunSync() should be > 1
      }
    }

    "return Success ExitCode even if Event Consumers Registry initialisation fails" in new TestCase {

      override val metrics        = new Metrics()
      override val gaugeScheduler = new Gauge()

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
      given(eventProducersRegistry).succeeds(returning = ())
      given(eventConsumersRegistry).fails(becauseOf = exceptions.generateOne)
      given(eventsQueue).succeeds(returning = ())
      given(httpServer).succeeds()

      startRunnerFor(2.seconds).unsafeRunSync() shouldBe ExitCode.Success

      eventually {
        metrics.counter.get.unsafeRunSync()        should be > 1
        gaugeScheduler.counter.get.unsafeRunSync() should be > 1
      }
    }

    "return Success ExitCode even if Event Log Metrics initialisation fails" in new TestCase {

      override val gaugeScheduler = new Gauge()

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      given(metrics).fails(becauseOf = exceptions.generateOne)
      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
      given(eventProducersRegistry).succeeds(returning = ())
      given(eventConsumersRegistry).succeeds(returning = ())
      given(eventsQueue).succeeds(returning = ())
      given(httpServer).succeeds()

      startRunnerFor(2.seconds).unsafeRunAndForget() shouldBe ()

      eventually {
        gaugeScheduler.counter.get.unsafeRunSync() should be > 1
      }
    }

    "return Success ExitCode even if StatusChangeEventQueue initialisation fails" in new TestCase {

      override val gaugeScheduler = new Gauge()

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      given(metrics).succeeds(returning = ())
      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
      given(eventProducersRegistry).succeeds(returning = ())
      given(eventConsumersRegistry).succeeds(returning = ())
      given(eventsQueue).fails(becauseOf = exceptions.generateOne)
      given(httpServer).succeeds()

      startRunnerFor(2.seconds).unsafeRunAndForget() shouldBe ()

      eventually {
        gaugeScheduler.counter.get.unsafeRunSync() should be > 1
      }
    }

    "return Success ExitCode even if Event Log Metrics scheduler initialisation fails" in new TestCase {

      override val metrics = new Metrics()

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
      given(eventProducersRegistry).succeeds(returning = ())
      given(eventConsumersRegistry).succeeds(returning = ())
      given(eventsQueue).succeeds(returning = ())
      given(gaugeScheduler).fails(becauseOf = exceptions.generateOne)
      given(httpServer).succeeds()

      startRunnerFor(3.seconds).unsafeRunAndForget()

      eventually {
        metrics.counter.get.unsafeRunSync() should be > 1
      }
    }
  }

  private trait TestCase {
    implicit val logger:         TestLogger[IO]              = TestLogger[IO]()
    val serviceReadinessChecker: ServiceReadinessChecker[IO] = new CountEffect
    val certificateLoader:       CertificateLoader[IO]       = new CountEffect
    val sentryInitializer:       SentryInitializer[IO]       = new CountEffect
    val dbInitializer:           DbInitializer[IO]           = new CountEffect
    val eventProducersRegistry:  EventProducersRegistry[IO]  = new CountEffect
    val eventConsumersRegistry:  EventConsumersRegistry[IO]  = new CountEffect
    val metrics:                 EventLogMetrics[IO]         = new CountEffect
    val eventsQueue:             StatusChangeEventsQueue[IO] = new CountEffect
    val httpServer = mock[HttpServer[IO]]
    val gaugeScheduler: GaugeResetScheduler[IO] = new CountEffect

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

    def startRunnerFor(duration: FiniteDuration): IO[ExitCode] =
      for {
        term <- SignallingRef.of[IO, Boolean](false)
        _    <- (IO.sleep(duration) *> term.set(true)).start
        exit <- runner.run(term)
      } yield exit

    def startRunnerForever: IO[ExitCode] =
      for {
        term <- SignallingRef.of[IO, Boolean](false)
        exit <- runner.run(term)
      } yield exit
  }

  private class Metrics extends EventLogMetrics[IO] {
    val counter = Ref.unsafe[IO, Int](0)

    override def run: IO[Unit] = counter.update(_ + 1)
  }

  private class Gauge extends GaugeResetScheduler[IO] {
    val counter = Ref.unsafe[IO, Int](0)

    override def run: IO[Unit] = keepGoing.foreverM

    private def keepGoing =
      Temporal[IO].delayBy(counter.update(_ + 1), 500 millis)
  }

  final class CountEffect
      extends ServiceReadinessChecker[IO]
      with CertificateLoader[IO]
      with SentryInitializer[IO]
      with DbInitializer[IO]
      with EventLogMetrics[IO]
      with EventProducersRegistry[IO]
      with EventConsumersRegistry[IO]
      with StatusChangeEventsQueue[IO]
      with GaugeResetScheduler[IO] {
    val counter = Ref.unsafe[IO, Int](0)

    def run: IO[Unit] = counter.update(_ + 1)

    override def waitIfNotUp: IO[Unit] = ???
    override def register(subscriptionRequest: Json): IO[EventProducersRegistry.SubscriptionResult] = ???
    override def getStatus: IO[Set[EventProducerStatus]] = ???
    override def handle(requestContent: EventRequestContent): IO[EventSchedulingResult] = ???
    override def renewAllSubscriptions(): IO[Unit] = ???
    override def register[E <: StatusChangeEvent](
        handler: E => IO[Unit]
    )(implicit decoder: Decoder[E], eventType: StatusChangeEventsQueue.EventType[E]): IO[Unit] = ???
    override def offer[E <: StatusChangeEvent](event: E)(implicit
        encoder:   Encoder[E],
        eventType: StatusChangeEventsQueue.EventType[E],
        show:      Show[E]
    ): Kleisli[IO, Session[IO], Unit] = ???
  }
}
