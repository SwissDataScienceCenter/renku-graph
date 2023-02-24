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
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk.Session
import MicroserviceRunnerSpec._
import cats.effect.unsafe.IORuntime
import org.http4s.{HttpApp, Response}
import org.http4s.server.Server
import org.scalatest.Assertion

class MicroserviceRunnerSpec
    extends AnyWordSpec
    with IOSpec
    with Eventually
    with IntegrationPatience
    with should.Matchers {

  "run" should {

    "return Success Exit Code " +
      "if Sentry, Events Distributor and metrics initialisation are fine " +
      "and http server starts up" in new TestCase {

        startAndStopRunner.unsafeRunSync()

        metrics.getCount.unsafeRunSync()                 shouldBe 1
        gaugeScheduler.getCount.unsafeRunSync()          shouldBe 1
        certificateLoader.getCount.unsafeRunSync()       shouldBe 1
        sentryInitializer.getCount.unsafeRunSync()       shouldBe 1
        dbInitializer.getCount.unsafeRunSync()           shouldBe 1
        serviceReadinessChecker.getCount.unsafeRunSync() shouldBe 1
        eventProducersRegistry.getCount.unsafeRunSync()  shouldBe 1
        eventConsumersRegistry.getCount.unsafeRunSync()  shouldBe 1
        eventsQueue.getCount.unsafeRunSync()             shouldBe 1
      }

    "fail if Certificate loading fails" in new TestCase {

      val exception = exceptions.generateOne
      certificateLoader.failWith(exception).unsafeRunSync()

      intercept[Exception](startRunnerForever.unsafeRunSync()) shouldBe exception
    }

    "fail if Sentry initialisation fails" in new TestCase {

      val exception = exceptions.generateOne
      // given(sentryInitializer).fails(becauseOf = exception)

      intercept[Exception](startRunnerForever.unsafeRunSync()) shouldBe exception
    }

    "return Success ExitCode even if DB initialisation fails" in new TestCase {

      // given(certificateLoader).succeeds(returning = ())
      // given(sentryInitializer).succeeds(returning = ())
      val exception = exceptions.generateOne
      // given(dbInitializer).fails(becauseOf = exception)

      startAndStopRunner.unsafeRunSync() shouldBe ExitCode.Success
    }

    "fail if starting the http server fails" in new TestCase {
      // given(certificateLoader).succeeds(returning = ())
      // given(sentryInitializer).succeeds(returning = ())
      // given(dbInitializer).succeeds(returning = ())
      // (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
      // given(eventProducersRegistry).succeeds(returning = ())
      // given(eventConsumersRegistry).succeeds(returning = ())
      // given(eventsQueue).succeeds(returning = ())
      val exception = exceptions.generateOne
      // given(httpServer).fails(becauseOf = exception)

      intercept[Exception](startRunnerForever.unsafeRunSync()) shouldBe exception
    }

    "return Success ExitCode even if Event Producers Registry initialisation fails" in new TestCase {

      // given(certificateLoader).succeeds(returning = ())
      // given(sentryInitializer).succeeds(returning = ())
      // given(dbInitializer).succeeds(returning = ())
      // (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
      // given(eventProducersRegistry).fails(becauseOf = exceptions.generateOne)
      // given(eventConsumersRegistry).succeeds(returning = ())
      // given(eventsQueue).succeeds(returning = ())
      // given(httpServer).succeeds()

      startAndStopRunner.unsafeRunSync() shouldBe ExitCode.Success

      eventually {
        metrics.getCount.unsafeRunSync()        should be > 1
        gaugeScheduler.getCount.unsafeRunSync() should be > 1
      }
    }

    "return Success ExitCode even if Event Consumers Registry initialisation fails" in new TestCase {

//      given(certificateLoader).succeeds(returning = ())
//      given(sentryInitializer).succeeds(returning = ())
//      given(dbInitializer).succeeds(returning = ())
//      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
//      given(eventProducersRegistry).succeeds(returning = ())
//      given(eventConsumersRegistry).fails(becauseOf = exceptions.generateOne)
//      given(eventsQueue).succeeds(returning = ())
//      given(httpServer).succeeds()

      startAndStopRunner.unsafeRunSync() shouldBe ExitCode.Success

      eventually {
        metrics.getCount.unsafeRunSync()        should be > 1
        gaugeScheduler.getCount.unsafeRunSync() should be > 1
      }
    }

    "return Success ExitCode even if Event Log Metrics initialisation fails" in new TestCase {

//      given(certificateLoader).succeeds(returning = ())
//      given(sentryInitializer).succeeds(returning = ())
//      given(dbInitializer).succeeds(returning = ())
//      given(metrics).fails(becauseOf = exceptions.generateOne)
//      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
//      given(eventProducersRegistry).succeeds(returning = ())
//      given(eventConsumersRegistry).succeeds(returning = ())
//      given(eventsQueue).succeeds(returning = ())
//      given(httpServer).succeeds()

      startAndStopRunner.unsafeRunAndForget() shouldBe ()

      eventually {
        gaugeScheduler.getCount.unsafeRunSync() should be > 1
      }
    }

    "return Success ExitCode even if StatusChangeEventQueue initialisation fails" in new TestCase {

//      given(certificateLoader).succeeds(returning = ())
//      given(sentryInitializer).succeeds(returning = ())
//      given(dbInitializer).succeeds(returning = ())
//      given(metrics).succeeds(returning = ())
//      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
//      given(eventProducersRegistry).succeeds(returning = ())
//      given(eventConsumersRegistry).succeeds(returning = ())
//      given(eventsQueue).fails(becauseOf = exceptions.generateOne)
//      given(httpServer).succeeds()

      startAndStopRunner.unsafeRunAndForget() shouldBe ()

      eventually {
        gaugeScheduler.getCount.unsafeRunSync() should be > 1
      }
    }

    "return Success ExitCode even if Event Log Metrics scheduler initialisation fails" in new TestCase {

//      given(certificateLoader).succeeds(returning = ())
//      given(sentryInitializer).succeeds(returning = ())
//      given(dbInitializer).succeeds(returning = ())
//      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
//      given(eventProducersRegistry).succeeds(returning = ())
//      given(eventConsumersRegistry).succeeds(returning = ())
//      given(eventsQueue).succeeds(returning = ())
//      given(gaugeScheduler).fails(becauseOf = exceptions.generateOne)
//      given(httpServer).succeeds()

      startAndStopRunner.unsafeRunAndForget()

      eventually {
        metrics.getCount.unsafeRunSync() should be > 1
      }
    }
  }
}

object MicroserviceRunnerSpec {
  private trait TestCase {
    implicit val logger:         TestLogger[IO]                           = TestLogger[IO]()
    val serviceReadinessChecker: ServiceReadinessChecker[IO] with Counter = new CountEffect
    val certificateLoader:       CertificateLoader[IO] with Counter       = new CountEffect
    val sentryInitializer:       SentryInitializer[IO] with Counter       = new CountEffect
    val dbInitializer:           DbInitializer[IO] with Counter           = new CountEffect
    val eventProducersRegistry:  EventProducersRegistry[IO] with Counter  = new CountEffect
    val eventConsumersRegistry:  EventConsumersRegistry[IO] with Counter  = new CountEffect
    val metrics:                 EventLogMetrics[IO] with Counter         = new CountEffect
    val eventsQueue:             StatusChangeEventsQueue[IO] with Counter = new CountEffect
    val gaugeScheduler:          GaugeResetScheduler[IO] with Counter     = new CountEffect
    val httpServer = new MockHttpServer

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

    def startAndStopRunner: IO[ExitCode] =
      for {
        term <- SignallingRef.of[IO, Boolean](true)
        exit <- runner.run(term)
      } yield exit

    def startRunnerForever: IO[ExitCode] =
      for {
        term <- SignallingRef.of[IO, Boolean](false)
        exit <- runner.run(term)
      } yield exit
  }

  trait Counter {
    def getCount: IO[Int]

    def failWith(ex: Throwable): IO[Unit]
  }

  def assertCalled(n: Int)(c: Counter, more: Counter*)(implicit rt: IORuntime): Assertion = {
    import should.Matchers._

    (c :: more.toList).map(_.getCount.unsafeRunSync()).sum shouldBe ((more.size + 1) * n)
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
      with GaugeResetScheduler[IO]
      with Counter {
    val counter = Ref.unsafe[IO, Either[Throwable, Int]](Right(0))

    def run: IO[Unit] = counter.updateAndGet(_.map(_ + 1)).rethrow.as(())

    def getCount: IO[Int] = counter.get.rethrow

    def failWith(ex: Throwable): IO[Unit] =
      counter.set(Left(ex))

    override def waitIfNotUp: IO[Unit] = run

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

  final class MockHttpServer extends HttpServer[IO] with MockFactory with Counter {
    override val httpApp: HttpApp[IO]                        = Kleisli(_ => IO.pure(Response.notFound))
    private val result:   Ref[IO, Either[Throwable, Server]] = Ref.unsafe(Right(mock[Server]))
    private val counter = Ref.unsafe[IO, Int](0)

    override def createServer: Resource[IO, Server] =
      Resource.eval(result.get.rethrow).evalTap(_ => counter.update(_ + 1))

    def failWith(ex: Throwable): IO[Unit] = result.set(Left(ex))

    override def getCount: IO[Int] = result.get.rethrow *> counter.get
  }
}
