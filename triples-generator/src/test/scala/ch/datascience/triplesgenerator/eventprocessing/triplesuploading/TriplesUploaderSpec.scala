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

package ch.datascience.triplesgenerator.eventprocessing.triplesuploading

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{FusekiBaseUrl, SparqlQueryTimeRecorder}
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.triplesgenerator.eventprocessing.triplesuploading.TriplesUploadResult._
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.auto._
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class TriplesUploaderSpec extends AnyWordSpec with MockFactory with ExternalServiceStubbing with should.Matchers {

  "upload" should {

    s"return $DeliverySuccess if uploading triples to the Store was successful" in new TestCase {

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/data")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/ld+json"))
          .withRequestBody(equalToJson(triples.value.toString()))
          .willReturn(ok())
      }

      triplesUploader.upload(triples).unsafeRunSync() shouldBe DeliverySuccess
    }

    (BadRequest +: InternalServerError +: Nil) foreach { status =>
      s"return $InvalidTriplesFailure if remote client responds with a $status" in new TestCase {
        val errorMessage = nonEmptyStrings().generateOne

        stubFor {
          post(s"/${rdfStoreConfig.datasetName}/data")
            .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
            .withHeader("content-type", equalTo("application/ld+json"))
            .withRequestBody(equalToJson(triples.value.toString()))
            .willReturn(badRequest().withBody(errorMessage))
        }

        triplesUploader.upload(triples).unsafeRunSync() shouldBe InvalidTriplesFailure(errorMessage)
      }
    }

    s"return $RecoverableFailure if remote responds with status different than $Ok, $BadRequest or $InternalServerError" in new TestCase {

      val errorMessage = nonEmptyStrings().generateOne

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/data")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/ld+json"))
          .withRequestBody(equalToJson(triples.value.toString()))
          .willReturn(unauthorized().withBody(errorMessage))
      }

      triplesUploader.upload(triples).unsafeRunSync() shouldBe RecoverableFailure(s"$Unauthorized: $errorMessage")
    }

    s"return $RecoverableFailure for connectivity issues" in new TestCase {

      val fusekiBaseUrl = localHttpUrls.map(FusekiBaseUrl.apply).generateOne
      override val rdfStoreConfig = rdfStoreConfigs.generateOne.copy(
        fusekiBaseUrl = fusekiBaseUrl
      )
      val exceptionMessage =
        s"Error connecting to $fusekiBaseUrl using address ${fusekiBaseUrl.toString.replaceFirst("http[s]?://", "")} (unresolved: false)"

      triplesUploader
        .upload(triples)
        .unsafeRunSync() shouldBe RecoverableFailure(
        s"POST $fusekiBaseUrl/${rdfStoreConfig.datasetName}/data error: $exceptionMessage"
      )
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {

    val triples = jsonLDTriples.generateOne

    val logger               = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val rdfStoreConfig = rdfStoreConfigs.generateOne.copy(
      fusekiBaseUrl = FusekiBaseUrl(externalServiceBaseUrl)
    )
    lazy val triplesUploader = new IOTriplesUploader(
      rdfStoreConfig,
      logger,
      timeRecorder,
      retryInterval = 100 millis,
      maxRetries = 1
    )
  }
}
