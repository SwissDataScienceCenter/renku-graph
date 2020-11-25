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

package ch.datascience.webhookservice

import java.util.concurrent.ConcurrentHashMap

import cats.effect._
import ch.datascience.config.certificates.CertificateLoader
import ch.datascience.generators.Generators
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.http.server.IOHttpServer
import ch.datascience.interpreters.IOSentryInitializer
import ch.datascience.testtools.MockedRunnableCollaborators
import ch.datascience.webhookservice.missedevents._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class MicroserviceRunnerSpec
    extends AnyWordSpec
    with MockedRunnableCollaborators
    with MockFactory
    with should.Matchers {

  "run" should {

    "return Success Exit Code if " +
      "Sentry initialisation is fine and " +
      "Events Synchronization Scheduler and Http Server start up" in new TestCase {

        given(certificateLoader).succeeds(returning = ())
        given(sentryInitializer).succeeds(returning = ())
        given(eventsSynchronizationScheduler).succeeds(returning = ())
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

    "fail if Sentry initialization fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      val exception = Generators.exceptions.generateOne
      given(sentryInitializer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "return Success Exit Code even if starting the Events Synchronization Scheduler fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      val exception = Generators.exceptions.generateOne
      given(eventsSynchronizationScheduler).fails(becauseOf = exception)
      given(httpServer).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }

    "fail if starting Http Server fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(eventsSynchronizationScheduler).succeeds(returning = ())
      val exception = Generators.exceptions.generateOne
      given(httpServer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    val certificateLoader              = mock[CertificateLoader[IO]]
    val sentryInitializer              = mock[IOSentryInitializer]
    val eventsSynchronizationScheduler = mock[TestIOEventsSynchronizationScheduler]
    val httpServer                     = mock[IOHttpServer]
    val runner = new MicroserviceRunner(
      certificateLoader,
      sentryInitializer,
      eventsSynchronizationScheduler,
      httpServer,
      new ConcurrentHashMap[CancelToken[IO], Unit]()
    )
  }
}
