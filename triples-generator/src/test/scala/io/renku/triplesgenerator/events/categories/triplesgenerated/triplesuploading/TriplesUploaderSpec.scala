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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplesuploading

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER
import eu.timepit.refined.auto._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.generators.jsonld.JsonLDGenerators._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{FusekiBaseUrl, SparqlQueryTimeRecorder}
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplesuploading.TriplesUploadResult.{DeliverySuccess, InvalidTriplesFailure, RecoverableFailure}
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.language.postfixOps

class TriplesUploaderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

  "upload" should {

    s"return $DeliverySuccess if uploading triples to the Store was successful" in new TestCase {

      givenUploader(returning = ok())

      triplesUploader.uploadTriples(triples).unsafeRunSync() shouldBe DeliverySuccess
    }

    (BadRequest +: InternalServerError +: Nil) foreach { status =>
      s"return $InvalidTriplesFailure if remote client responds with a $status" in new TestCase {
        val errorMessage = nonEmptyStrings().generateOne

        givenUploader(returning = aResponse().withStatus(status.code).withBody(errorMessage))

        triplesUploader.uploadTriples(triples).unsafeRunSync() shouldBe InvalidTriplesFailure(errorMessage)
      }
    }

    s"return $RecoverableFailure if remote responds with status different than $Ok, $BadRequest or $InternalServerError" in new TestCase {

      val errorMessage = nonEmptyStrings().generateOne

      givenUploader(returning = unauthorized().withBody(errorMessage))

      triplesUploader.uploadTriples(triples).unsafeRunSync() shouldBe RecoverableFailure(
        s"$Unauthorized: $errorMessage"
      )
    }

    s"return $RecoverableFailure for connectivity issues" in new TestCase {

      givenUploader(returning = aResponse.withFault(CONNECTION_RESET_BY_PEER))

      val failure = triplesUploader.uploadTriples(triples).unsafeRunSync()

      failure       shouldBe a[RecoverableFailure]
      failure.message should startWith(s"POST $externalServiceBaseUrl/${rdfStoreConfig.datasetName}/data error")
    }
  }

  private trait TestCase {

    val triples = jsonLDEntities.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder[IO]())
    lazy val rdfStoreConfig  = rdfStoreConfigs.generateOne.copy(fusekiBaseUrl = FusekiBaseUrl(externalServiceBaseUrl))
    lazy val triplesUploader =
      new TriplesUploaderImpl[IO](rdfStoreConfig, timeRecorder, retryInterval = 100 millis, maxRetries = 1)

    def givenUploader(returning: ResponseDefinitionBuilder) = stubFor {
      post(s"/${rdfStoreConfig.datasetName}/data")
        .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
        .withHeader("content-type", equalTo("application/ld+json"))
        .withRequestBody(equalToJson(triples.toJson.toString()))
        .willReturn(returning)
    }
  }
}
