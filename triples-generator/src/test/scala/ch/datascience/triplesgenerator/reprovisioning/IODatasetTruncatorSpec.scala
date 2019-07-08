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

package ch.datascience.triplesgenerator.reprovisioning

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import org.http4s.Status
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IODatasetTruncatorSpec extends WordSpec with ExternalServiceStubbing {

  "truncateDataset" should {

    "succeed when SPARQL truncate command sent to the Store's update endpoint got OK" in new TestCase {

      stubFor {
        post(s"/${fusekiConfig.datasetName}/update")
          .withBasicAuth(fusekiConfig.authCredentials.username.value, fusekiConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
          .withRequestBody(equalTo("update=DELETE { ?s ?p ?o} WHERE { ?s ?p ?o}"))
          .willReturn(ok())
      }

      datasetTruncator.truncateDataset.unsafeRunSync() shouldBe ((): Unit)
    }

    "fails when SPARQL truncate command sent to the Store's update endpoint got status other than OK" in new TestCase {

      stubFor {
        post(s"/${fusekiConfig.datasetName}/update")
          .willReturn(badRequest().withBody("some message"))
      }

      intercept[Exception] {
        datasetTruncator.truncateDataset.unsafeRunSync()
      }.getMessage shouldBe s"POST $fusekiBaseUrl/${fusekiConfig.datasetName}/update returned ${Status.BadRequest}; body: some message"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val fusekiBaseUrl = FusekiBaseUrl(externalServiceBaseUrl)
    val fusekiConfig  = rdfStoreConfigs.generateOne.copy(fusekiBaseUrl = fusekiBaseUrl)
    val logger        = TestLogger[IO]()

    val datasetTruncator = new IODatasetTruncator(fusekiConfig, logger)
  }
}
