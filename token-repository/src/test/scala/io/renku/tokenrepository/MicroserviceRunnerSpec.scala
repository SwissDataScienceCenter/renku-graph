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
import io.renku.microservices.{AbstractMicroserviceRunnerTest, CallCounter, ServiceRunCounter}
import io.renku.tokenrepository.MicroserviceRunnerSpec.CountEffect
import io.renku.tokenrepository.repository.init.DbInitializer
import org.http4s.HttpRoutes
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._

class MicroserviceRunnerSpec extends AnyWordSpec with should.Matchers {

  "run" should {

    "initialise Sentry, load certificate, initialise DB and start HTTP server" in new TestCase {

      startFor(1.second).unsafeRunSync() shouldBe ExitCode.Success

      logger.loggedOnly(Info("Service started"))

      assertCalledAll.unsafeRunSync()
    }

    "fail if Certificate loading fails" in new TestCase {

      val exception = exceptions.generateOne
      certificateLoader.failWith(exception).unsafeRunSync()

      intercept[Exception] {
        startRunnerForever.unsafeRunSync()
      } shouldBe exception
    }

    "fail if Sentry initialization fails" in new TestCase {

      val exception = exceptions.generateOne
      sentryInitializer.failWith(exception).unsafeRunSync()

      intercept[Exception] {
        startRunnerForever.unsafeRunSync()
      } shouldBe exception
    }

    "return Success ExitCode even if DB initialisation fails as the process is run in another thread" in new TestCase {

      val exception = exceptions.generateOne
      dbInitializer.failWith(exception).unsafeRunSync()

      startFor(1.second).unsafeRunSync() shouldBe ExitCode.Success
      assertCalledAllBut(dbInitializer, microserviceRoutes).unsafeRunSync()
      assertNotCalled(microserviceRoutes).unsafeRunSync()

      logger.logged(Error("DB initialization failed", exception), Info("Service started"))
    }

    "fail if starting the http server fails" in new TestCase {

      val exception = exceptions.generateOne
      httpServer.failWith(exception).unsafeRunSync()

      intercept[Exception] {
        startRunnerForever.unsafeRunSync()
      } shouldBe exception

      assertCalledAllBut(httpServer).unsafeRunSync()
    }
  }

  private trait TestCase extends AbstractMicroserviceRunnerTest {
    implicit val logger:    TestLogger[IO]                          = TestLogger[IO]()
    val certificateLoader:  CertificateLoader[IO] with CallCounter  = new CountEffect("CertificateLoader")
    val sentryInitializer:  SentryInitializer[IO] with CallCounter  = new CountEffect("SentryInitializer")
    val dbInitializer:      DbInitializer[IO] with CallCounter      = new CountEffect("DbInitializer")
    val httpServer:         HttpServer[IO] with CallCounter         = new CountEffect("HttpServer")
    val microserviceRoutes: MicroserviceRoutes[IO] with CallCounter = new CountEffect("MicroServiceRoutes")

    val runner =
      new MicroserviceRunner(certificateLoader, sentryInitializer, dbInitializer, httpServer, microserviceRoutes)

    val all: List[CallCounter] = List(
      certificateLoader,
      sentryInitializer,
      dbInitializer,
      httpServer,
      microserviceRoutes
    )
  }
}

object MicroserviceRunnerSpec {
  private class CountEffect(name: String)
      extends ServiceRunCounter(name)
      with DbInitializer[IO]
      with MicroserviceRoutes[IO] {
    override def notifyDBReady(): IO[Unit]                     = run
    override def routes:          Resource[IO, HttpRoutes[IO]] = ???
  }
}
