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

package io.renku.tokenrepository

import cats.effect._
import cats.effect.unsafe.implicits.global
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.server.HttpServer
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.MockedRunnableCollaborators
import io.renku.tokenrepository.repository.init.DbInitializer
import org.http4s.HttpRoutes
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.language.reflectiveCalls

class MicroserviceRunnerSpec
    extends AnyWordSpec
    with MockedRunnableCollaborators
    with Eventually
    with MockFactory
    with should.Matchers {

  "run" should {

    "initialise Sentry, load certificate, initialise DB and start HTTP server" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      given(Runnable(httpServer.run)).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success

      logger.loggedOnly(Info("Service started"))

      verifyRoutesNotified
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
      val exception = exceptions.generateOne
      given(sentryInitializer).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }

    "return Success ExitCode even if DB initialisation fails as the process is run in another thread" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(dbInitializer).fails(becauseOf = exception)
      given(Runnable(httpServer.run)).succeeds(returning = ExitCode.Success)

      runner.run().unsafeRunSync() shouldBe ExitCode.Success

      eventually {
        logger.logged(Error("DB initialization failed", exception), Info("Service started"))
      }

      microserviceRoutes.notifyDBCalled.get.unsafeRunSync() shouldBe false
    }

    "fail if starting the http server fails" in new TestCase {

      given(certificateLoader).succeeds(returning = ())
      given(sentryInitializer).succeeds(returning = ())
      given(dbInitializer).succeeds(returning = ())
      val exception = exceptions.generateOne
      given(Runnable(httpServer.run)).fails(becauseOf = exception)

      intercept[Exception] {
        runner.run().unsafeRunSync()
      } shouldBe exception
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val certificateLoader = mock[CertificateLoader[IO]]
    val sentryInitializer = mock[SentryInitializer[IO]]
    val dbInitializer     = mock[DbInitializer[IO]]
    val httpServer        = mock[HttpServer[IO]]
    val microserviceRoutes = new MicroserviceRoutes[IO] {
      val notifyDBCalled: Ref[IO, Boolean] = Ref.unsafe(false)
      override def notifyDBReady() = notifyDBCalled.set(true)
      override def routes          = Resource.pure(HttpRoutes.empty[IO])
    }
    val runner =
      new MicroserviceRunner(certificateLoader, sentryInitializer, dbInitializer, httpServer, microserviceRoutes)

    def verifyRoutesNotified = eventually {
      microserviceRoutes.notifyDBCalled.get.unsafeRunSync() shouldBe true
    }
  }
}
