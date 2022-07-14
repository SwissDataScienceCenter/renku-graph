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

package io.renku.rdfstore

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.RdfStoreAdminClient.CreationResult
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status._
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger

class RdfStoreAdminClientSpec
    extends AnyWordSpec
    with should.Matchers
    with ExternalServiceStubbing
    with IOSpec
    with TableDrivenPropertyChecks {

  "createDataset" should {

    forAll(
      Table(
        "response code" -> "result",
        Ok              -> CreationResult.Created,
        Conflict        -> CreationResult.Existed
      )
    ) { case (responseCode, result) =>
      "post the given DatasetConfigFile to the /datasets admin endpoint " +
        s"and return $result if TS responds with $responseCode" in new TestCase {

          val dsConfigFile = datasetConfigFiles.generateOne

          stubFor {
            post(s"/$$/datasets")
              .withRequestBody(equalTo(dsConfigFile.show))
              .withBasicAuth(adminConnectionConfig.authCredentials.username.value,
                             adminConnectionConfig.authCredentials.password.value
              )
              .withHeader("content-type", equalTo("text/turtle"))
              .willReturn(
                aResponse
                  .withStatus(responseCode.code)
                  .withBody("some message")
              )
          }

          client.createDataset(dsConfigFile).unsafeRunSync() shouldBe result
        }
    }

    "fail when TS responds with status different than Ok or Conflict" in new TestCase {

      val dsConfigFile = datasetConfigFiles.generateOne

      stubFor {
        post(s"/$$/datasets")
          .withRequestBody(equalTo(dsConfigFile.show))
          .withBasicAuth(adminConnectionConfig.authCredentials.username.value,
                         adminConnectionConfig.authCredentials.password.value
          )
          .withHeader("content-type", equalTo("text/turtle"))
          .willReturn(
            aResponse
              .withStatus(BadRequest.code)
              .withBody("some message")
          )
      }

      val exception = intercept[Exception] {
        client.createDataset(dsConfigFile).unsafeRunSync()
      }

      exception.getMessage should include(BadRequest.toString)
    }
  }

  private trait TestCase {
    val fusekiUrl             = FusekiUrl(externalServiceBaseUrl)
    val adminConnectionConfig = adminConnectionConfigs.generateOne.copy(fusekiUrl = fusekiUrl)
    implicit val logger:       Logger[IO]                  = TestLogger[IO]()
    implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val client = new RdfStoreAdminClientImpl[IO](adminConnectionConfig)
  }
}
