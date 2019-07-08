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

package ch.datascience.triplesgenerator.eventprocessing

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import com.github.tomakehurst.wiremock.client.WireMock._
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

class TriplesUploaderSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "upload" should {

    "succeed if upload the triples to Jena Fuseki is successful" in new TestCase {

      stubFor {
        post(s"/${fusekiUserConfig.datasetName}/data")
          .withBasicAuth(fusekiUserConfig.authCredentials.username.value,
                         fusekiUserConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/rdf+xml"))
          .withRequestBody(equalTo(rdfTriples.value))
          .willReturn(ok())
      }

      triplesUploader.upload(rdfTriples).unsafeRunSync() shouldBe ((): Unit)
    }

    "return a RuntimeException if remote client responds with status different than OK" in new TestCase {

      stubFor {
        post(s"/${fusekiUserConfig.datasetName}/data")
          .withBasicAuth(fusekiUserConfig.authCredentials.username.value,
                         fusekiUserConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/rdf+xml"))
          .withRequestBody(equalTo(rdfTriples.value))
          .willReturn(unauthorized().withBody("some error"))
      }

      intercept[Exception] {
        triplesUploader.upload(rdfTriples).unsafeRunSync()
      }.getMessage shouldBe s"POST ${fusekiUserConfig.fusekiBaseUrl}/${fusekiUserConfig.datasetName}/data returned ${Status.Unauthorized}; body: some error"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {

    val rdfTriples = rdfTriplesSets.generateOne

    val fusekiUserConfig = rdfStoreConfigs.generateOne.copy(
      fusekiBaseUrl = FusekiBaseUrl(externalServiceBaseUrl)
    )
    val triplesUploader = new IOTriplesUploader(fusekiUserConfig, TestLogger())
  }
}
