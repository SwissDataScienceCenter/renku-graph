/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.server.HttpServer
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import io.renku.microservices.{AbstractMicroserviceRunnerTest, CallCounter, ServiceReadinessChecker, ServiceRunCounter}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.MicroserviceRunnerSpec.CountEffect
import io.renku.triplesgenerator.config.certificates.GitCertificateInstaller
import io.renku.triplesgenerator.init.CliVersionCompatibilityVerifier
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._

class MicroserviceRunnerSpec extends AnyWordSpec with IOSpec with should.Matchers {

  "run" should {

    "return Success ExitCode if " +
      "Sentry and TS datasets initialisations are fine " +
      "and subscription, re-provisioning and the http server start up" in new TestCase {

        startFor(1.second).unsafeRunSync() shouldBe ExitCode.Success
        assertCalledAll.unsafeRunSync()
      }

    "fail if Certificate loading fails" in new TestCase {

      val exception = exceptions.generateOne
      certificateLoader.failWith(exception).unsafeRunSync()

      intercept[Exception] {
        startRunnerForever.unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "fail if installing Certificate for Git fails" in new TestCase {

      val exception = exceptions.generateOne
      gitCertificateInstaller.failWith(exception).unsafeRunSync()

      intercept[Exception] {
        startRunnerForever.unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "fail if Sentry initialization fails" in new TestCase {

      val exception = exceptions.generateOne
      sentryInitializer.failWith(exception).unsafeRunSync()

      intercept[Exception] {
        startRunnerForever.unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "fail if cli version compatibility fails" in new TestCase {
      val exception = exceptions.generateOne
      cliVersionCompatChecker.failWith(exception).unsafeRunSync()

      intercept[Exception] {
        startRunnerForever.unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "fail if starting the Http Server fails" in new TestCase {

      val exception = exceptions.generateOne
      httpServer.failWith(exception).unsafeRunSync()

      intercept[Exception] {
        startRunnerForever.unsafeRunSync()
      } shouldBe exception

      logger.loggedOnly(
        Error(exception.getMessage, exception)
      )
    }

    "return Success ExitCode even when running eventConsumersRegistry fails" in new TestCase {

      eventConsumersRegistry.failWith(exceptions.generateOne).unsafeRunSync()

      startFor(1.second).unsafeRunSync() shouldBe ExitCode.Success
      assertCalledAllBut(eventConsumersRegistry).unsafeRunSync()
    }
  }

  private trait TestCase extends AbstractMicroserviceRunnerTest {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val serviceReadinessChecker: ServiceReadinessChecker[IO] with CallCounter = new CountEffect(
      "ServiceReadinessChecker"
    )
    val certificateLoader: CertificateLoader[IO] with CallCounter = new CountEffect("CertificateLoader")
    val gitCertificateInstaller: GitCertificateInstaller[IO] with CallCounter = new CountEffect(
      "GitCertificateInstaller"
    )
    val sentryInitializer: SentryInitializer[IO] with CallCounter = new CountEffect("SentryInitializer")
    val cliVersionCompatChecker: CliVersionCompatibilityVerifier[IO] with CallCounter = new CountEffect(
      "CliVersionCompatChecker"
    )
    val eventConsumersRegistry: EventConsumersRegistry[IO] with CallCounter = new CountEffect("EventConsumerRegistry")
    val httpServer:             HttpServer[IO] with CallCounter             = new CountEffect("HttpServer")

    val runner = new MicroserviceRunner(serviceReadinessChecker,
                                        certificateLoader,
                                        gitCertificateInstaller,
                                        sentryInitializer,
                                        cliVersionCompatChecker,
                                        eventConsumersRegistry,
                                        httpServer
    )

    lazy val all: List[CallCounter] = List(
      serviceReadinessChecker,
      certificateLoader,
      gitCertificateInstaller,
      sentryInitializer,
      eventConsumersRegistry,
      httpServer
    )
  }
}

object MicroserviceRunnerSpec {
  class CountEffect(name: String)
      extends ServiceRunCounter(name)
      with GitCertificateInstaller[IO]
      with CliVersionCompatibilityVerifier[IO]
}
