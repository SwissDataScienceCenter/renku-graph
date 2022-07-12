/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.triplesuploading

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
import io.renku.rdfstore.{FusekiUrl, SparqlQueryTimeRecorder}
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError._
import io.renku.triplesgenerator.events.consumers.tsprovisioning.triplesuploading.TriplesUploadResult._
import org.http4s.Status._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class TriplesUploaderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

  "upload" should {

    s"succeeds if uploading triples to the Store was successful" in new TestCase {

      givenUploader(returning = ok())

      triplesUploader.uploadTriples(triples).value.unsafeRunSync() shouldBe ().asRight
    }

    s"fail if remote client responds with a $BadRequest" in new TestCase {
      val errorMessage = nonEmptyStrings().generateOne

      givenUploader(returning = aResponse().withStatus(BadRequest.code).withBody(errorMessage))

      intercept[BadRequestException] {
        triplesUploader.uploadTriples(triples).value.unsafeRunSync()
      }
    }

    Set(Forbidden, Unauthorized) foreach { status =>
      s"return Auth $RecoverableFailure if remote responds with $status " in new TestCase {

        val errorMessage = nonEmptyStrings().generateOne
        givenUploader(returning = aResponse().withStatus(status.code).withBody(errorMessage))

        val Left(error) = triplesUploader.uploadTriples(triples).value.unsafeRunSync()
        error shouldBe a[SilentRecoverableError]
      }
    }

    s"return Log-worthy $RecoverableFailure if remote responds with status different than " +
      s"OK, BAD_REQUEST, FORBIDDEN, OR UNAUTHORIZED" in new TestCase {

        val errorMessage = nonEmptyStrings().generateOne
        givenUploader(returning = serviceUnavailable().withBody(errorMessage))

        val Left(error) = triplesUploader.uploadTriples(triples).value.unsafeRunSync()
        error shouldBe a[LogWorthyRecoverableError]
      }

    s"return Log-worthy $RecoverableFailure for connectivity issues" in new TestCase {

      givenUploader(returning = aResponse.withFault(CONNECTION_RESET_BY_PEER))

      val Left(failure) = triplesUploader.uploadTriples(triples).value.unsafeRunSync()

      failure shouldBe a[LogWorthyRecoverableError]
      failure.getMessage should startWith(
        s"POST $externalServiceBaseUrl/${renkuConnectionConfig.datasetName}/data error"
      )
    }
  }

  private trait TestCase {

    val triples = jsonLDEntities.generateOne

    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    lazy val renkuConnectionConfig =
      renkuConnectionConfigs.generateOne.copy(fusekiUrl = FusekiUrl(externalServiceBaseUrl))
    lazy val triplesUploader =
      new TriplesUploaderImpl[IO](renkuConnectionConfig, retryInterval = 100 millis, maxRetries = 1)

    def givenUploader(returning: ResponseDefinitionBuilder) = stubFor {
      post(s"/${renkuConnectionConfig.datasetName}/data")
        .withBasicAuth(renkuConnectionConfig.authCredentials.username.value,
                       renkuConnectionConfig.authCredentials.password.value
        )
        .withHeader("content-type", equalTo("application/ld+json"))
        .withRequestBody(equalToJson(triples.toJson.toString()))
        .willReturn(returning)
    }
  }
}
