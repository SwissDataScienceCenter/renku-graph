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
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.server.IOHttpServer
import ch.datascience.interpreters.TestLogger.Level.{Error, Warn}
import ch.datascience.interpreters.{IOSentryInitializer, TestLogger}
import ch.datascience.testtools.MockedRunnableCollaborators
import ch.datascience.triplesgenerator.config.RenkuPythonDevVersion
import ch.datascience.triplesgenerator.config.certificates.GitCertificateInstaller
import ch.datascience.triplesgenerator.init.{CliVersionCompatibilityVerifier, IOFusekiDatasetInitializer}
import ch.datascience.triplesgenerator.reprovisioning.ReProvisioning
import ch.datascience.triplesgenerator.subscriptions.Subscriber
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import cats.syntax.all._
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext

class MicroserviceRunnerSpec
    extends AnyWordSpec
    with MockedRunnableCollaborators
    with MockFactory
    with should.Matchers {

  "run" should {

    "return Success ExitCode if " +
      "Sentry and RDF dataset initialisation are fine " +
      "and subscription, re-provisioning and the http server start up" in new TestCase {

        given(certificateLoader).succeeds(returning = ())
        given(gitCertificateInstaller).succeeds(returning = ())
        given(sentryInitializer).succeeds(returning = ())
        given(cliVersionCompatChecker).succeeds(returning = ())
        given(datasetInitializer).succeeds(returning = ())
        given(subscriber).succeeds(returning = ())
        given(reProvisioning).succeeds(returning = ())
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

    "fail if cli version compatibility fails" in new TestCase {
      given(certificateLoader).succeeds(returning = ())
      given(gitCertificateInstaller).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(cliVersionCompatChecker) fails (becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "fail if RDF dataset verification fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(gitCertificateInstaller).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(cliVersionCompatChecker).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(datasetInitializer).fails(becauseOf = exception)

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
      given(cliVersionCompatChecker).succeeds(returning = ())
      given(datasetInitializer).succeeds(returning = ())
      given(subscriber).succeeds(returning = ())
      given(reProvisioning).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(httpServer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "return Success ExitCode even if running Subscriber fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(gitCertificateInstaller).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(cliVersionCompatChecker).succeeds(returning = ())
      given(datasetInitializer).succeeds(returning = ())
      given(subscriber).fails(becauseOf = exceptions.generateOne)
      given(reProvisioning).succeeds(returning = ())
      given(httpServer).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }

    "return Success ExitCode even if starting re-provisioning process fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(gitCertificateInstaller).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(cliVersionCompatChecker).succeeds(returning = ())
      given(datasetInitializer).succeeds(returning = ())
      given(subscriber).succeeds(returning = ())
      given(reProvisioning).fails(becauseOf = exceptions.generateOne)
      given(httpServer).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success
    }

    "return Success Exit code but not run VersionCompatibilityChecker when renkuPythonDevVersion defined" in new TestCase {
      val runnerWithRenkuPythonDevVersion = new MicroserviceRunner(
        certificateLoader,
        gitCertificateInstaller,
        sentryInitializer,
        maybeRenkuPythonDevVersion = RenkuPythonDevVersion(nonEmptyStrings().generateOne).some,
        cliVersionCompatChecker,
        datasetInitializer,
        subscriber,
        reProvisioning,
        httpServer,
        new ConcurrentHashMap[CancelToken[IO], Unit](),
        logger
      )

      given(certificateLoader).succeeds(returning = ())
      given(gitCertificateInstaller).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(datasetInitializer).succeeds(returning = ())
      given(subscriber).succeeds(returning = ())
      given(reProvisioning).succeeds(returning = ())
      given(httpServer).succeeds(returning = ExitCode.Success)

      runnerWithRenkuPythonDevVersion.run().unsafeRunSync() shouldBe ExitCode.Success

      logger.loggedOnly(
        Warn(
          s"RENKU_PYTHON_DEV_VERSION env variable is set. No version compatibility check will take place"
        )
      )
    }
  }

  private trait TestCase {
    val certificateLoader       = mock[CertificateLoader[IO]]
    val gitCertificateInstaller = mock[GitCertificateInstaller[IO]]
    val sentryInitializer       = mock[IOSentryInitializer]
    val cliVersionCompatChecker = mock[CliVersionCompatibilityVerifier[IO]]
    val datasetInitializer      = mock[IOFusekiDatasetInitializer]
    val subscriber              = mock[Subscriber[IO]]
    val reProvisioning          = mock[ReProvisioning[IO]]
    val httpServer              = mock[IOHttpServer]
    val logger                  = TestLogger[IO]()

    val runner = new MicroserviceRunner(
      certificateLoader,
      gitCertificateInstaller,
      sentryInitializer,
      maybeRenkuPythonDevVersion = None,
      cliVersionCompatChecker,
      datasetInitializer,
      subscriber,
      reProvisioning,
      httpServer,
      new ConcurrentHashMap[CancelToken[IO], Unit](),
      logger
    )
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer: Timer[IO]        = IO.timer(ExecutionContext.global)
}
