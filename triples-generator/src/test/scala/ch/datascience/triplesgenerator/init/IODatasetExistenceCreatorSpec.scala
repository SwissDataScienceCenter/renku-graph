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
import ch.datascience.config.ServiceUrl
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import com.github.tomakehurst.wiremock.client.WireMock._
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IODatasetExistenceCreatorSpec extends WordSpec with ExternalServiceStubbing with MockFactory {

  "createDataset" should {

    "succeed if POST /$/datasets returns OK" in new TestCase {

      stubFor {
        post("/$/datasets")
          .withHeader("content-type", containing("application/x-www-form-urlencoded"))
          .withBasicAuth(fusekiConfig.username.value, fusekiConfig.password.value)
          .withRequestBody(equalTo(s"dbName=${fusekiConfig.datasetName}&dbType=${fusekiConfig.datasetType}"))
          .willReturn(ok())
      }

      datasetExistenceCreator.createDataset(fusekiConfig).unsafeRunSync() shouldBe ((): Unit)
    }

    "fail if POST /$/datasets returns status different than OK" in new TestCase {

      stubFor {
        post("/$/datasets")
          .willReturn(unauthorized().withBody("some message"))
      }

      intercept[Exception] {
        datasetExistenceCreator.createDataset(fusekiConfig).unsafeRunSync()
      }.getMessage shouldBe s"POST $fusekiBaseUrl/$$/datasets returned ${Status.Unauthorized}; body: some message"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val fusekiBaseUrl = ServiceUrl(externalServiceBaseUrl)
    val fusekiConfig  = fusekiConfigs.generateOne.copy(fusekiBaseUrl = fusekiBaseUrl)

    val datasetExistenceCreator = new IODatasetExistenceCreator(TestLogger())
  }
}
