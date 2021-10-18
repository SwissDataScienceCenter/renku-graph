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

package io.renku.eventlog.subscriptions.zombieevents

import cats.effect.{ContextShift, IO, Timer}
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.httpUrls
import io.renku.interpreters.TestLogger
import io.renku.microservices.MicroserviceBaseUrl
import io.renku.stubbing.ExternalServiceStubbing
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class ServiceHealthCheckerSpec extends AnyWordSpec with ExternalServiceStubbing with should.Matchers {

  "ping" should {

    "return true if the service reponds with Ok" in new TestCase {
      stubFor {
        get("/ping").willReturn(ok())
      }

      healthChecker.ping(microserviceUrl).unsafeRunSync() shouldBe true
    }

    "return false if the service responds with status different than Ok" in new TestCase {
      stubFor {
        get("/ping").willReturn(notFound())
      }

      healthChecker.ping(microserviceUrl).unsafeRunSync() shouldBe false
    }

    "return there's connectivity problem to the service" in new TestCase {
      healthChecker.ping(MicroserviceBaseUrl(httpUrls().generateOne)).unsafeRunSync() shouldBe false
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val microserviceUrl = MicroserviceBaseUrl(externalServiceBaseUrl)
    val healthChecker   = new ServiceHealthCheckerImpl[IO](TestLogger(), retryInterval = 50 millis, maxRetries = 1)
  }
}
