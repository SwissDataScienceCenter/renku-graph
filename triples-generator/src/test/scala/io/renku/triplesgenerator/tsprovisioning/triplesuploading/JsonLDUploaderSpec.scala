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

package io.renku.triplesgenerator.tsprovisioning.triplesuploading

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER
import eu.timepit.refined.auto._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.generators.jsonld.JsonLDGenerators._
import io.renku.http.client.RestClientError.BadRequestException
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.errors.ProcessingRecoverableError._
import io.renku.triplesgenerator.tsprovisioning.triplesuploading.TriplesUploadResult._
import io.renku.triplesstore.{FusekiUrl, SparqlQueryTimeRecorder}
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class JsonLDUploaderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

  "uploadJsonLD" should {

    "succeeds if uploading triples to the Store was successful" in new TestCase {

      givenUploader(returning = ok())

      uploader.uploadJsonLD(triples).value.unsafeRunSync() shouldBe ().asRight
    }

    s"fail if remote client responds with a $BadRequest" in new TestCase {
      val errorMessage = nonEmptyStrings().generateOne

      givenUploader(returning = aResponse().withStatus(BadRequest.code).withBody(errorMessage))

      intercept[BadRequestException] {
        uploader.uploadJsonLD(triples).value.unsafeRunSync()
      }
    }

    Set(Forbidden, Unauthorized) foreach { status =>
      s"return Auth $RecoverableFailure if remote responds with $status " in new TestCase {

        val errorMessage = nonEmptyStrings().generateOne
        givenUploader(returning = aResponse().withStatus(status.code).withBody(errorMessage))

        val Left(error) = uploader.uploadJsonLD(triples).value.unsafeRunSync()
        error shouldBe a[SilentRecoverableError]
      }
    }

    s"return Log-worthy $RecoverableFailure if remote responds with status different than " +
      s"OK, BAD_REQUEST, FORBIDDEN, OR UNAUTHORIZED" in new TestCase {

        val errorMessage = nonEmptyStrings().generateOne
        givenUploader(returning = serviceUnavailable().withBody(errorMessage))

        val Left(error) = uploader.uploadJsonLD(triples).value.unsafeRunSync()
        error shouldBe a[LogWorthyRecoverableError]
      }

    s"return Log-worthy $RecoverableFailure for connectivity issues" in new TestCase {

      givenUploader(returning = aResponse.withFault(CONNECTION_RESET_BY_PEER))

      val Left(failure) = uploader.uploadJsonLD(triples).value.unsafeRunSync()

      failure shouldBe a[LogWorthyRecoverableError]
      failure.getMessage should startWith(
        s"POST $externalServiceBaseUrl/${storeConfig.datasetName}/data error"
      )
    }
  }

  private trait TestCase {

    val triples = jsonLDEntities.generateOne

    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    lazy val storeConfig = storeConnectionConfigs.generateOne.copy(fusekiUrl = FusekiUrl(externalServiceBaseUrl))
    lazy val uploader    = new JsonLDUploaderImpl[IO](storeConfig, retryInterval = 100 millis, maxRetries = 1)

    def givenUploader(returning: ResponseDefinitionBuilder) = stubFor {
      post(s"/${storeConfig.datasetName}/data")
        .withBasicAuth(storeConfig.authCredentials.username.value, storeConfig.authCredentials.password.value)
        .withHeader("content-type", equalTo("application/ld+json"))
        .withRequestBody(equalToJson(triples.toJson.toString()))
        .willReturn(returning)
    }
  }
}
