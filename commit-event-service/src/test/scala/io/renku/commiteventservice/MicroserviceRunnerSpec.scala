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

package io.renku.commiteventservice

import cats.effect._
import io.renku.config.certificates.CertificateLoader
import io.renku.config.sentry.SentryInitializer
import io.renku.events.consumers.EventConsumersRegistry
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.server.HttpServer
import io.renku.interpreters.TestLogger
import io.renku.microservices.{AbstractMicroserviceRunnerTest, CallCounter, ServiceReadinessChecker, ServiceRunCounter}
import io.renku.testtools.IOSpec

import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._

class MicroserviceRunnerSpec extends AnyWordSpec with IOSpec with should.Matchers {

  "run" should {

    "return Success Exit Code " +
      "if certificate loader, sentry, events consumer and metrics initialisation are fine " +
      "and http server starts up" in new TestCase {

        startFor(1.second).unsafeRunSync() shouldBe ExitCode.Success
        assertCalledAll.unsafeRunSync()
      }

    "fail if certificate loading fails" in new TestCase {

      val exception = exceptions.generateOne
      certificateLoader.failWith(exception).unsafeRunSync()

      intercept[Exception] {
        startRunnerForever.unsafeRunSync()
      } shouldBe exception
    }

    "fail if sentry initialisation fails" in new TestCase {

      val exception = exceptions.generateOne
      sentryInitializer.failWith(exception).unsafeRunSync()

      intercept[Exception] {
        startRunnerForever.unsafeRunSync()
      } shouldBe exception
    }

    "fail if starting the http server fails" in new TestCase {

      val exception = exceptions.generateOne
      httpServer.failWith(exception).unsafeRunSync()

      intercept[Exception](startRunnerForever.unsafeRunSync()) shouldBe exception
    }

    "return Success ExitCode even if Event Consumers Registry initialisation fails" in new TestCase {

      eventConsumersRegistry.failWith(exceptions.generateOne).unsafeRunSync()

      startFor(500.millis).unsafeRunSync() shouldBe ExitCode.Success
      assertCalledAllBut(eventConsumersRegistry, httpServer).unsafeRunSync()
    }
  }

  private trait TestCase extends AbstractMicroserviceRunnerTest {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val serviceReadinessChecker: ServiceReadinessChecker[IO] with CallCounter = new ServiceRunCounter(
      "ServiceReadinessChecker"
    )
    val certificateLoader: CertificateLoader[IO] with CallCounter = new ServiceRunCounter("CertificateLoader")
    val sentryInitializer: SentryInitializer[IO] with CallCounter = new ServiceRunCounter("SentryInitializer")
    val eventConsumersRegistry: EventConsumersRegistry[IO] with CallCounter = new ServiceRunCounter(
      "EventConsumerRegistry"
    )
    val httpServer: HttpServer[IO] with CallCounter = new ServiceRunCounter("HttpServer")
    val runner = new MicroserviceRunner(serviceReadinessChecker,
                                        certificateLoader,
                                        sentryInitializer,
                                        eventConsumersRegistry,
                                        httpServer
    )

    val all: List[CallCounter] = List(
      serviceReadinessChecker,
      certificateLoader,
      sentryInitializer,
      eventConsumersRegistry,
      httpServer
    )
  }
}
