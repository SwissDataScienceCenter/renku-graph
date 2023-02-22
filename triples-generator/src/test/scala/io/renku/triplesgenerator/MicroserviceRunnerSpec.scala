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

package io.renku.triplesgenerator

import cats.effect._
import cats.syntax.all._
import fs2.concurrent.SignallingRef
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.server.HttpServer
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.microservices.ServiceReadinessChecker
import io.renku.testtools.{IOSpec, MockedRunnableCollaborators}
import io.renku.triplesgenerator.config.certificates.GitCertificateInstaller
import io.renku.triplesgenerator.init.CliVersionCompatibilityVerifier
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class MicroserviceRunnerSpec
    extends AnyWordSpec
    with IOSpec
    with MockedRunnableCollaborators
    with MockFactory
    with should.Matchers {

  "run" should {

    "return Success ExitCode if " +
      "Sentry and TS datasets initialisations are fine " +
      "and subscription, re-provisioning and the http server start up" in new TestCase {

        (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
        given(certificateLoader).succeeds(returning = ())
        given(gitCertificateInstaller).succeeds(returning = ())
        given(sentryInitializer).succeeds(returning = ())
        given(cliVersionCompatChecker).succeeds(returning = ())
        given(eventConsumersRegistry).succeeds(returning = ())
        given(httpServer).succeeds()

        startRunnerFor(1 second).unsafeRunSync() shouldBe ExitCode.Success
      }

    "fail if Certificate loading fails" in new TestCase {

      val exception = exceptions.generateOne
      given(certificateLoader).fails(becauseOf = exception)

      intercept[Exception] {
        startRunnerForever.unsafeRunSync()
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
        startRunnerForever.unsafeRunSync()
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
        startRunnerForever.unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "fail if cli version compatibility fails" in new TestCase {
      given(certificateLoader).succeeds(returning = ())
      given(gitCertificateInstaller).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(cliVersionCompatChecker) fails (becauseOf = exception)

      intercept[Exception] {
        startRunnerForever.unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "fail if starting the Http Server fails" in new TestCase {

      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
      given(certificateLoader).succeeds(returning = ())
      given(gitCertificateInstaller).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(cliVersionCompatChecker).succeeds(returning = ())
      given(eventConsumersRegistry).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(httpServer).fails(becauseOf = exception)

      intercept[Exception] {
        startRunnerFor(1 second).unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "return Success ExitCode even when running eventConsumersRegistry fails" in new TestCase {

      (() => serviceReadinessChecker.waitIfNotUp).expects().returning(().pure[IO])
      given(certificateLoader).succeeds(returning = ())
      given(gitCertificateInstaller).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(cliVersionCompatChecker).succeeds(returning = ())
      given(eventConsumersRegistry).fails(becauseOf = exceptions.generateOne)
      given(httpServer).succeeds()

      startRunnerFor(1 second).unsafeRunSync() shouldBe ExitCode.Success
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val serviceReadinessChecker = mock[ServiceReadinessChecker[IO]]
    val certificateLoader       = mock[CertificateLoader[IO]]
    val gitCertificateInstaller = mock[GitCertificateInstaller[IO]]
    val sentryInitializer       = mock[SentryInitializer[IO]]
    val cliVersionCompatChecker = mock[CliVersionCompatibilityVerifier[IO]]
    val eventConsumersRegistry  = mock[EventConsumersRegistry[IO]]
    val httpServer              = mock[HttpServer[IO]]

    val runner = new MicroserviceRunner(serviceReadinessChecker,
                                        certificateLoader,
                                        gitCertificateInstaller,
                                        sentryInitializer,
                                        cliVersionCompatChecker,
                                        eventConsumersRegistry,
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
}
