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

package ch.datascience.triplesgenerator

import cats.effect._
import ch.datascience.config.certificates.CertificateLoader
import ch.datascience.events.consumers.EventConsumersRegistry
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.server.IOHttpServer
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.interpreters.{IOSentryInitializer, TestLogger}
import ch.datascience.testtools.MockedRunnableCollaborators
import ch.datascience.triplesgenerator.config.certificates.GitCertificateInstaller
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext

class MicroserviceRunnerSpec
    extends AnyWordSpec
    with MockedRunnableCollaborators
    with MockFactory
    with should.Matchers {

  "run" should {

    "return Success ExitCode if " +
      "Sentry is fine " +
      "and subscription, and the http server start up" in new TestCase {

        given(certificateLoader).succeeds(returning = ())
        given(gitCertificateInstaller).succeeds(returning = ())
        given(sentryInitializer).succeeds(returning = ())
        given(eventConsumersRegistry).succeeds(returning = ())
        given(httpServer).succeeds(returning = ExitCode.Success)

        runner.run().unsafeRunSync() shouldBe ExitCode.Success
      }

    "fail if Certificate loading fails" in new TestCase {

      val exception = exceptions.generateOne
      given(certificateLoader).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "fail if installing Certificate for Git fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(gitCertificateInstaller).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "fail if Sentry initialization fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(gitCertificateInstaller).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(sentryInitializer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "fail if starting the Http Server fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(gitCertificateInstaller).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(eventConsumersRegistry).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(httpServer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "return Success ExitCode even if running subscriptionMechanismRegistry fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(gitCertificateInstaller).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(eventConsumersRegistry).fails(becauseOf = exceptions.generateOne)
      given(httpServer).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }
  }

  private trait TestCase {
    val certificateLoader       = mock[CertificateLoader[IO]]
    val gitCertificateInstaller = mock[GitCertificateInstaller[IO]]
    val sentryInitializer       = mock[IOSentryInitializer]
    val eventConsumersRegistry  = mock[EventConsumersRegistry[IO]]
    val httpServer              = mock[IOHttpServer]
    val logger                  = TestLogger[IO]()

    val runner = new MicroserviceRunner(
      certificateLoader,
      gitCertificateInstaller,
      sentryInitializer,
      eventConsumersRegistry,
      httpServer,
      new ConcurrentHashMap[CancelToken[IO], Unit](),
      logger
    )
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer: Timer[IO]        = IO.timer(ExecutionContext.global)
}
