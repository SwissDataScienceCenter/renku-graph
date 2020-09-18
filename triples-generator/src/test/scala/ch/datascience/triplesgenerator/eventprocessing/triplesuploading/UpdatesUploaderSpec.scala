/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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
import ch.datascience.generators.CommonGraphGenerators.rdfStoreConfigs
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{FusekiBaseUrl, SparqlQuery, SparqlQueryTimeRecorder}
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplesuploading.TriplesUploadResult.{DeliverySuccess, InvalidUpdatesFailure, RecoverableFailure}
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.auto._
import org.http4s.Status
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class UpdatesUploaderSpec extends AnyWordSpec with ExternalServiceStubbing with should.Matchers {

  "send" should {

    s"return $DeliverySuccess if all the given updates pass" in new TestCase {
      givenStore(forUpdate = query, returning = ok())

      updater.send(query).unsafeRunSync() shouldBe DeliverySuccess
    }

    s"return $InvalidUpdatesFailure if the given updates is invalid (RDF store responds with BAD_REQUEST 400)" in new TestCase {

      val errorMessage = nonEmptyStrings().generateOne
      givenStore(forUpdate = query, returning = badRequest().withBody(errorMessage))

      updater.send(query).unsafeRunSync() shouldBe InvalidUpdatesFailure(
        s"Triples curation update '${query.name}' failed: $errorMessage"
      )
    }

    s"return $RecoverableFailure if remote responds with status different than OK or BAD_REQUEST" in new TestCase {

      val errorMessage = nonEmptyStrings().generateOne
      givenStore(forUpdate = query, returning = serviceUnavailable().withBody(errorMessage))

      updater.send(query).unsafeRunSync() shouldBe RecoverableFailure(
        s"Triples curation update failed: ${Status.ServiceUnavailable}: $errorMessage"
      )
    }

    s"return $RecoverableFailure for connectivity issues" in new TestCase {

      val fusekiBaseUrl = localHttpUrls.map(FusekiBaseUrl.apply).generateOne
      override val rdfStoreConfig = rdfStoreConfigs.generateOne.copy(
        fusekiBaseUrl = fusekiBaseUrl
      )
      val connectionExceptionMessage =
        s"Error connecting to $fusekiBaseUrl using address ${fusekiBaseUrl.toString.replaceFirst("http[s]?://", "")} (unresolved: false)"
      updater
        .send(query)
        .unsafeRunSync() shouldBe RecoverableFailure(
        s"POST $fusekiBaseUrl/${rdfStoreConfig.datasetName}/update error: $connectionExceptionMessage"
      )
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val query                = sparqlQueries.generateOne
    val logger               = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val rdfStoreConfig = rdfStoreConfigs.generateOne.copy(
      fusekiBaseUrl = FusekiBaseUrl(externalServiceBaseUrl)
    )
    lazy val updater = new IOUpdatesUploader(
      rdfStoreConfig,
      logger,
      timeRecorder,
      retryInterval = 100 millis,
      maxRetries = 1
    )

    def givenStore(forUpdate: SparqlQuery, returning: ResponseDefinitionBuilder) =
      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/update")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
          .withRequestBody(equalTo(s"update=${urlEncode(forUpdate.toString)}"))
          .willReturn(returning)
      }
  }

}
