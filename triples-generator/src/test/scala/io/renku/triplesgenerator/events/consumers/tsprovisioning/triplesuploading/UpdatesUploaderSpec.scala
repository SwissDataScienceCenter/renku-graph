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
import io.renku.generators.CommonGraphGenerators.{rdfStoreConfigs, sparqlQueries}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.{FusekiBaseUrl, SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError._
import io.renku.triplesgenerator.events.consumers.tsprovisioning.triplesuploading.TriplesUploadResult._
import org.http4s.Status.{Forbidden, Unauthorized}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class UpdatesUploaderSpec extends AnyWordSpec with IOSpec with ExternalServiceStubbing with should.Matchers {

  "send" should {

    s"return $DeliverySuccess if all the given updates pass" in new TestCase {
      givenStore(forUpdate = query, returning = ok())

      updater.send(query).value.unsafeRunSync() shouldBe ().asRight
    }

    s"return $NonRecoverableFailure if the given updates is invalid (RDF store responds with BAD_REQUEST 400)" in new TestCase {

      givenStore(forUpdate = query, returning = badRequest())

      intercept[NonRecoverableFailure](updater.send(query).value.unsafeRunSync()).getMessage should startWith(
        "Triples transformation update 'curation update' failed"
      )
    }

    Set(Forbidden, Unauthorized) foreach { status =>
      s"return a SilentRecoverableFailure if remote responds with $status " in new TestCase {

        val errorMessage = nonEmptyStrings().generateOne
        givenStore(forUpdate = query, returning = aResponse().withStatus(status.code).withBody(errorMessage))

        val Left(error) = updater.send(query).value.unsafeRunSync()
        error          shouldBe a[SilentRecoverableError]
        error.getMessage should startWith("Triples transformation update 'curation update' failed:")
      }
    }

    s"return Log-worthy $RecoverableFailure if remote responds with status different than " +
      s"OK, BAD_REQUEST, FORBIDDEN, OR UNAUTHORIZED" in new TestCase {

        val errorMessage = nonEmptyStrings().generateOne
        givenStore(forUpdate = query, returning = serviceUnavailable().withBody(errorMessage))

        val Left(error) = updater.send(query).value.unsafeRunSync()

        error          shouldBe a[LogWorthyRecoverableError]
        error.getMessage should startWith("Triples transformation update 'curation update' failed:")
      }

    s"return Log-worthy $RecoverableFailure for connectivity issues" in new TestCase {

      givenStore(forUpdate = query, returning = aResponse.withFault(CONNECTION_RESET_BY_PEER))

      val Left(failure) = updater.send(query).value.unsafeRunSync()

      failure shouldBe a[LogWorthyRecoverableError]
      failure.getMessage should startWith(
        s"Triples transformation update 'curation update' failed: POST $externalServiceBaseUrl/${rdfStoreConfig.datasetName}/update error"
      )
    }
  }

  private trait TestCase {
    val query = sparqlQueries.generateOne

    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    lazy val rdfStoreConfig = rdfStoreConfigs.generateOne.copy(fusekiBaseUrl = FusekiBaseUrl(externalServiceBaseUrl))
    lazy val updater        = new UpdatesUploaderImpl[IO](rdfStoreConfig, retryInterval = 100 millis, maxRetries = 1)

    def givenStore(forUpdate: SparqlQuery, returning: ResponseDefinitionBuilder) = stubFor {
      post(s"/${rdfStoreConfig.datasetName}/update")
        .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
        .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
        .withRequestBody(equalTo(s"update=${urlEncode(forUpdate.toString)}"))
        .willReturn(returning)
    }
  }
}
