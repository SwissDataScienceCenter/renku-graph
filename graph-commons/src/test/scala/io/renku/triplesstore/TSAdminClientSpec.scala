/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesstore

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.MappingBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.triplesstore.TSAdminClient.{CreationResult, RemovalResult}
import org.http4s.Status
import org.http4s.Status._
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger

class TSAdminClientSpec
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
              .withAuth
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
          .withAuth
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

  "checkDatasetExists" should {

    "return true if TS' admin API to fetch dataset info returns Ok" in new TestCase {

      val datasetName = nonEmptyStrings().generateAs(DatasetName)

      stubFor {
        get(show"/$$/datasets/$datasetName").withAuth
          .willReturn(
            okJson(json"""{ 
              "ds.name" : ${"/" + datasetName.show} ,
              "ds.state" : true ,
              "ds.services" : []
            }""".spaces2)
          )
      }

      client.checkDatasetExists(datasetName).unsafeRunSync() shouldBe true
    }

    "return false if TS' admin API to fetch dataset info returns NotFound" in new TestCase {

      val datasetName = nonEmptyStrings().generateAs(DatasetName)

      stubFor {
        get(show"/$$/datasets/$datasetName").withAuth
          .willReturn(
            aResponse().withStatus(NotFound.code)
          )
      }

      client.checkDatasetExists(datasetName).unsafeRunSync() shouldBe false

    }

    "fail for other response codes" in new TestCase {

      val datasetName = nonEmptyStrings().generateAs(DatasetName)

      stubFor {
        get(show"/$$/datasets/$datasetName").withAuth
          .willReturn(
            aResponse().withStatus(BadRequest.code)
          )
      }

      val exception = intercept[Exception] {
        client.checkDatasetExists(datasetName).unsafeRunSync()
      }

      exception.getMessage should include(BadRequest.toString)
    }
  }

  "removeDataset" should {

    val datasetName = nonEmptyStrings().generateAs(DatasetName)

    forAll {
      Table(
        "status"                -> "expected result",
        Status.Ok               -> RemovalResult.Removed,
        Status.NotFound         -> RemovalResult.NotExisted,
        Status.MethodNotAllowed -> RemovalResult.NotAllowed
      )
    } { (status, result) =>
      "send a DELETE request to delete the dataset with the given name " +
        s"and return $result for $status" in new TestCase {

          stubFor {
            delete(s"/$$/datasets/$datasetName").withAuth
              .willReturn(aResponse.withStatus(status.code))
          }

          (client removeDataset datasetName).unsafeRunSync() shouldBe result
        }
    }

    "fail for other response statuses" in new TestCase {

      stubFor {
        delete(s"/$$/datasets/$datasetName").withAuth
          .willReturn(aResponse.withStatus(BadRequest.code))
      }

      val exception = intercept[Exception] {
        client.removeDataset(datasetName).unsafeRunSync()
      }

      exception.getMessage should include(BadRequest.toString)
    }
  }

  private trait TestCase {
    val fusekiUrl             = FusekiUrl(externalServiceBaseUrl)
    val adminConnectionConfig = adminConnectionConfigs.generateOne.copy(fusekiUrl = fusekiUrl)
    implicit val logger: Logger[IO] = TestLogger[IO]()
    val client = new TSAdminClientImpl[IO](adminConnectionConfig)

    implicit class MappingBuilderOps(builder: MappingBuilder) {

      def withAuth: MappingBuilder = builder.withBasicAuth(adminConnectionConfig.authCredentials.username.value,
                                                           adminConnectionConfig.authCredentials.password.value
      )
    }
  }
}
