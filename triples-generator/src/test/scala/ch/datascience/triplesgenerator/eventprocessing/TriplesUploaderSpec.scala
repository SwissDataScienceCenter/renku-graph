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
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.triplesgenerator.eventprocessing.TriplesUploadResult._
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.auto._
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class TriplesUploaderSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "upload" should {

    "return success if upload to the Triples Store was successful" in new TestCase {

      stubFor {
        post(s"/${fusekiUserConfig.datasetName}/data")
          .withBasicAuth(fusekiUserConfig.authCredentials.username.value,
                         fusekiUserConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/ld+json"))
          .withRequestBody(equalToJson(triples.value.toString()))
          .willReturn(ok())
      }

      triplesUploader.upload(triples).unsafeRunSync() shouldBe TriplesUploaded
    }

    "return MalformedTriples if remote client responds with BAD_REQUEST" in new TestCase {

      val errorMessage = nonEmptyStrings().generateOne

      stubFor {
        post(s"/${fusekiUserConfig.datasetName}/data")
          .withBasicAuth(fusekiUserConfig.authCredentials.username.value,
                         fusekiUserConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/ld+json"))
          .withRequestBody(equalToJson(triples.value.toString()))
          .willReturn(badRequest().withBody(errorMessage))
      }

      triplesUploader.upload(triples).unsafeRunSync() shouldBe MalformedTriples(errorMessage)
    }

    "return UploadingError if remote client responds with status different than OK or BAD_REQUEST" in new TestCase {

      val errorMessage = nonEmptyStrings().generateOne

      stubFor {
        post(s"/${fusekiUserConfig.datasetName}/data")
          .withBasicAuth(fusekiUserConfig.authCredentials.username.value,
                         fusekiUserConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/ld+json"))
          .withRequestBody(equalToJson(triples.value.toString()))
          .willReturn(unauthorized().withBody(errorMessage))
      }

      triplesUploader.upload(triples).unsafeRunSync() shouldBe UploadingError(s"$Unauthorized: $errorMessage")
    }

    "return UploadingError for connectivity issues" in new TestCase {

      val fusekiBaseUrl = localHttpUrls.map(FusekiBaseUrl.apply).generateOne
      override val fusekiUserConfig = rdfStoreConfigs.generateOne.copy(
        fusekiBaseUrl = fusekiBaseUrl
      )

      triplesUploader
        .upload(triples)
        .unsafeRunSync() shouldBe UploadingError(
        s"POST $fusekiBaseUrl/${fusekiUserConfig.datasetName}/data error: Connection refused"
      )
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {

    val triples = jsonLDTriples.generateOne

    val fusekiUserConfig = rdfStoreConfigs.generateOne.copy(
      fusekiBaseUrl = FusekiBaseUrl(externalServiceBaseUrl)
    )
    lazy val triplesUploader = new IOTriplesUploader(
      fusekiUserConfig,
      TestLogger(),
      retryInterval = 100 millis,
      maxRetries    = 1
    )
  }
}
