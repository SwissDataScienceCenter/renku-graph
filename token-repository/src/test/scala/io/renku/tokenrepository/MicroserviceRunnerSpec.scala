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
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.server.HttpServer
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.microservices.{AbstractMicroserviceRunnerTest, CallCounter, ServiceRunCounter}
import io.renku.tokenrepository.MicroserviceRunnerSpec.CountEffect
import io.renku.tokenrepository.repository.cleanup.ExpiringTokensRemover
import io.renku.tokenrepository.repository.init.DbInitializer
import org.http4s.HttpRoutes
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, Succeeded}

import scala.concurrent.duration._

class MicroserviceRunnerSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers {

  "run" should {

    "initialise Sentry, load certificate, initialise DB, kick off token removal and start HTTP server" in runnerTest {
      runner =>
        for {
          _ <- runner.startFor(1.second).asserting(_ shouldBe ExitCode.Success)

          _ <- runner.logger.loggedOnlyF(Info("Service started"))

          res <- runner.assertCalledAll.assertNoException
        } yield res
    }

    "fail if Certificate loading fails" in runnerTest { runner =>
      val exception = exceptions.generateOne
      runner.certificateLoader.failWith(exception) >>
        runner.startRunnerForever.assertThrowsError[Exception](_ shouldBe exception)
    }

    "fail if Sentry initialization fails" in runnerTest { runner =>
      val exception = exceptions.generateOne
      runner.sentryInitializer.failWith(exception) >>
        runner.startRunnerForever.assertThrowsError[Exception](_ shouldBe exception)
    }

    "return Success ExitCode even if DB initialisation fails as the process is run in another thread" in runnerTest {
      runner =>
        val exception = exceptions.generateOne
        for {
          _ <- runner.dbInitializer.failWith(exception)

          _ <- runner.startFor(1.second).asserting(_ shouldBe ExitCode.Success)
          _ <- runner.assertCalledAllBut(runner.dbInitializer, runner.expiringTokensRemover, runner.microserviceRoutes)
          _ <- runner.assertNotCalled(runner.expiringTokensRemover, runner.microserviceRoutes)

          _ <- runner.logger.loggedF(Error("DB initialization failed", exception), Info("Service started"))
        } yield Succeeded
    }

    "return Success ExitCode even if Expiring tokens removal fails as the process is run in another thread" in runnerTest {
      runner =>
        val exception = exceptions.generateOne
        for {
          _ <- runner.expiringTokensRemover.failWith(exception)

          _ <- runner.startFor(2.seconds).asserting(_ shouldBe ExitCode.Success)
          _ <- runner.assertCalledAllBut(runner.expiringTokensRemover)

          _ <- runner.logger.loggedF(Error("Expiring Tokens Removal failed", exception), Info("Service started"))
        } yield Succeeded
    }

    "fail if starting the http server fails" in runnerTest { runner =>
      val exception = exceptions.generateOne
      runner.httpServer.failWith(exception) >>
        runner.startRunnerForever.assertThrowsError[Exception](_ shouldBe exception) >>
        runner.assertCalledAllBut(runner.httpServer).assertNoException
    }
  }

  private class RunnerTest extends AbstractMicroserviceRunnerTest {
    implicit val logger:       TestLogger[IO]                             = TestLogger[IO]()
    val certificateLoader:     CertificateLoader[IO] with CallCounter     = new CountEffect("CertificateLoader")
    val sentryInitializer:     SentryInitializer[IO] with CallCounter     = new CountEffect("SentryInitializer")
    val dbInitializer:         DbInitializer[IO] with CallCounter         = new CountEffect("DbInitializer")
    val expiringTokensRemover: ExpiringTokensRemover[IO] with CallCounter = new CountEffect("ExpiringTokensRemover")
    val httpServer:            HttpServer[IO] with CallCounter            = new CountEffect("HttpServer")
    val microserviceRoutes:    MicroserviceRoutes[IO] with CallCounter    = new CountEffect("MicroServiceRoutes")

    val runner = new MicroserviceRunner(certificateLoader,
                                        sentryInitializer,
                                        dbInitializer,
                                        expiringTokensRemover,
                                        httpServer,
                                        microserviceRoutes
    )

    val all: List[CallCounter] = List(
      certificateLoader,
      sentryInitializer,
      dbInitializer,
      expiringTokensRemover,
      httpServer,
      microserviceRoutes
    )
  }

  private def runnerTest(f: RunnerTest => IO[Assertion]) = f(new RunnerTest)
}

object MicroserviceRunnerSpec {
  private class CountEffect(name: String)
      extends ServiceRunCounter(name)
      with DbInitializer[IO]
      with ExpiringTokensRemover[IO]
      with MicroserviceRoutes[IO] {
    override def notifyDBReady():        IO[Unit]                     = run
    override def removeExpiringTokens(): IO[Unit]                     = run
    override def routes:                 Resource[IO, HttpRoutes[IO]] = ???
  }
}
