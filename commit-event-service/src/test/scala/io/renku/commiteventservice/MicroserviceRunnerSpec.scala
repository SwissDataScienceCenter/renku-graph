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

package io.renku.commiteventservice

import cats.effect._
import cats.syntax.all._
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.events.EventRequestContent
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.server.HttpServer
import io.renku.interpreters.TestLogger
import io.renku.microservices.ServiceReadinessChecker
import io.renku.testtools.{IOSpec, MockedRunnableCollaborators}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

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
      "if certificate loader, sentry, events consumer and metrics initialisation are fine " +
      "and http server starts up" in new TestCase {

        given(certificateLoader).succeeds(returning = ())
        given(sentryInitializer).succeeds(returning = ())
        (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
        given(httpServer).succeeds(returning = ExitCode.Success)

        runner.run().unsafeRunSync() shouldBe ExitCode.Success

        eventually {
          eventConsumersRegistry.counter.get.unsafeRunSync() should be > 1
        }
      }

    "fail if certificate loading fails" in new TestCase {

      val exception = exceptions.generateOne
      given(certificateLoader).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if sentry initialisation fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(sentryInitializer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "fail if starting the http server fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
      val exception = exceptions.generateOne
      given(httpServer).fails(becauseOf = exception)

      intercept[Exception](runner.run().unsafeRunSync()) shouldBe exception
    }

    "return Success ExitCode even if Event Consumers Registry initialisation fails" in new TestCase {

      override val eventConsumersRegistry = new EventsConsumersRegistryStub(exceptions.generateSome)

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
      given(httpServer).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val serviceReadinessChecker = mock[ServiceReadinessChecker[IO]]
    val certificateLoader       = mock[CertificateLoader[IO]]
    val sentryInitializer       = mock[SentryInitializer[IO]]
    val eventConsumersRegistry  = new EventsConsumersRegistryStub()
    val httpServer              = mock[HttpServer[IO]]
    val runner = new MicroserviceRunner(serviceReadinessChecker,
                                        certificateLoader,
                                        sentryInitializer,
                                        eventConsumersRegistry,
                                        httpServer
    )
  }

  private class EventsConsumersRegistryStub(maybeFail: Option[Exception] = None) extends EventConsumersRegistry[IO] {
    val counter = Ref.unsafe[IO, Int](0)

    override def run(): IO[Unit] = maybeFail.map(_.raiseError[IO, Unit]).getOrElse(keepGoing.foreverM)

    private def keepGoing = Temporal[IO].delayBy(counter.update(_ + 1), 1000 millis)

    override def handle(requestContent: EventRequestContent) = fail("Shouldn't be called")
    override def renewAllSubscriptions()                     = fail("Shouldn't be called")
  }
}
