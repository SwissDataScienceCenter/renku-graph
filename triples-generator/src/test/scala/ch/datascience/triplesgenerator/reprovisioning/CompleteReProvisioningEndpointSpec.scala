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

package ch.datascience.triplesgenerator.reprovisioning

import cats.MonadError
import cats.effect.{IO, Timer}
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.http.client.BasicAuthCredentials
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.http.server.EndpointTester._
import ch.datascience.triplesgenerator.generators.ServiceTypesGenerators._
import com.typesafe.config.ConfigFactory
import io.circe.Json
import io.circe.syntax._
import org.http4s.MediaType.application
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.{Authorization, `Content-Type`}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class CompleteReProvisioningEndpointSpec extends WordSpec with MockFactory {

  "reProvisionAll" should {

    "return ACCEPTED when a valid credentials are given " +
      "and re-provisioning got triggered" in new TestCase {

      val request = Request[IO](Method.DELETE, uri"triples" / "projects")
        .withHeaders(Headers.of(Authorization(basicCredentials)))

      (reProvisioner.startReProvisioning _)
        .expects()
        .returning(context.unit)

      val response = reProvisionAll(request).unsafeRunSync

      response.status                 shouldBe Accepted
      response.contentType            shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync shouldBe InfoMessage("RDF store re-provisioning started").asJson
    }

    "return UNAUTHORIZED when no valid credentials are given" in new TestCase {

      val request = Request[IO](Method.DELETE, uri"triples" / "projects")

      val response = reProvisionAll(request).unsafeRunSync

      response.status                 shouldBe Unauthorized
      response.contentType            shouldBe Some(`Content-Type`(application.json))
      response.as[Json].unsafeRunSync shouldBe ErrorMessage(UnauthorizedException.getMessage).asJson
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    private val authCredentials = basicAuthCredentials.generateOne
    val basicCredentials        = BasicCredentials(authCredentials.username.value, authCredentials.password.value)
    val reProvisioner           = mock[IOReProvisioner]
    val reProvisionAll          = new CompleteReProvisioningEndpoint[IO](authCredentials, reProvisioner).reProvisionAll _
  }
}

class IOCompleteReProvisioningEndpointSpec extends WordSpec with MockFactory {

  import scala.collection.JavaConverters._
  private implicit val timer: Timer[IO] = IO.timer(global)

  "apply" should {

    "instantiate CompleteReProvisionEndpoint with basic auth credentials read from config" in {
      val fusekiConfig = fusekiConfigs.generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "username"     -> fusekiConfig.authCredentials.username.value,
              "password"     -> fusekiConfig.authCredentials.password.value,
              "url"          -> fusekiConfig.fusekiBaseUrl.value,
              "dataset-name" -> fusekiConfig.datasetName.value,
              "dataset-type" -> fusekiConfig.datasetType.value
            ).asJava
          ).asJava
        ).asJava
      )

      val transactor = DbTransactor[IO, EventLogDB](null)
      val endpoint   = IOCompleteReProvisionEndpoint(transactor, config).unsafeRunSync()

      endpoint.credentials shouldBe BasicAuthCredentials(fusekiConfig.authCredentials.username,
                                                         fusekiConfig.authCredentials.password)
    }

    "fail if fuseki config cannot be read from the config" in {
      val fusekiConfig = fusekiConfigs.generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "services" -> Map(
            "fuseki" -> Map(
              "username"     -> fusekiConfig.authCredentials.username.value,
              "password"     -> fusekiConfig.authCredentials.password.value,
              "url"          -> fusekiConfig.fusekiBaseUrl.value,
              "dataset-name" -> fusekiConfig.datasetName.value,
              "dataset-type" -> fusekiConfig.datasetType.value
            ).removeRandomEntry.asJava
          ).asJava
        ).asJava
      )

      val transactor = DbTransactor[IO, EventLogDB](null)
      intercept[ConfigLoadingException] {
        IOCompleteReProvisionEndpoint(transactor, config).unsafeRunSync()
      }
    }
  }

  private implicit class ConfigMapOps(config: Map[String, String]) {
    private val configToMiss = Random.nextInt(config.size)

    lazy val removeRandomEntry: Map[String, String] =
      config.zipWithIndex
        .foldLeft(Map.empty[String, String]) {
          case (newConfig, (_, `configToMiss`)) => newConfig
          case (newConfig, ((k, v), _))         => newConfig + (k -> v)
        }
  }
}
