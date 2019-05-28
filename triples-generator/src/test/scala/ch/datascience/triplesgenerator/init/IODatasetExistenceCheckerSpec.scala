/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.init

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.triplesgenerator.config.FusekiBaseUrl
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import com.github.tomakehurst.wiremock.client.WireMock._
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IODatasetExistenceCheckerSpec extends WordSpec with ExternalServiceStubbing with MockFactory {

  "doesDatasetExists" should {

    "return true if client responds with OK" in new TestCase {

      stubFor {
        get(s"/$$/datasets/${fusekiConfig.datasetName}")
          .withBasicAuth(fusekiConfig.authCredentials.username.value, fusekiConfig.authCredentials.password.value)
          .willReturn(ok())
      }

      datasetExistenceChecker.doesDatasetExists(fusekiConfig).unsafeRunSync() shouldBe true
    }

    "return false if client responds with NOT_FOUND" in new TestCase {

      stubFor {
        get(s"/$$/datasets/${fusekiConfig.datasetName}")
          .withBasicAuth(fusekiConfig.authCredentials.username.value, fusekiConfig.authCredentials.password.value)
          .willReturn(notFound())
      }

      datasetExistenceChecker.doesDatasetExists(fusekiConfig).unsafeRunSync() shouldBe false
    }

    "fail if client responds with neither OK nor NOT_FOUND" in new TestCase {

      stubFor {
        get(s"/$$/datasets/${fusekiConfig.datasetName}")
          .withBasicAuth(fusekiConfig.authCredentials.username.value, fusekiConfig.authCredentials.password.value)
          .willReturn(unauthorized().withBody("some message"))
      }

      intercept[Exception] {
        datasetExistenceChecker.doesDatasetExists(fusekiConfig).unsafeRunSync()
      }.getMessage shouldBe s"GET $fusekiBaseUrl/$$/datasets/${fusekiConfig.datasetName} returned ${Status.Unauthorized}; body: some message"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val fusekiBaseUrl = FusekiBaseUrl(externalServiceBaseUrl)
    val fusekiConfig  = fusekiConfigs.generateOne.copy(fusekiBaseUrl = fusekiBaseUrl)

    val datasetExistenceChecker = new IODatasetExistenceChecker(TestLogger())
  }
}
