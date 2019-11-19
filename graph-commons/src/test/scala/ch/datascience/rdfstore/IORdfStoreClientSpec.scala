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

package ch.datascience.rdfstore

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.http.client.IORestClient
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Json
import org.http4s.Status.{BadRequest, Ok}
import org.http4s.{Request, Response, Status}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

class IORdfStoreClientSpec extends WordSpec with ExternalServiceStubbing with MockFactory {

  "IORdfStoreClient" should {

    "be a IORestClient" in new QueryClientTestCase {
      client shouldBe a[IORdfStoreClient]
      client shouldBe a[IORestClient[_]]
    }
  }

  "send sparql query" should {

    "succeed returning decoded response if the remote responds with OK and expected body type" in new QueryClientTestCase {

      val responseBody = jsons.generateOne

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
          .withHeader("accept", equalTo("application/sparql-results+json"))
          .withRequestBody(equalTo(s"query=${client.query}"))
          .willReturn(okJson(responseBody.noSpaces))
      }

      client.callRemote.unsafeRunSync() shouldBe responseBody
    }

    "fail if remote responds with non-OK status" in new QueryClientTestCase {

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .willReturn(
            aResponse
              .withStatus(BadRequest.code)
              .withBody("some message")
          )
      }

      intercept[Exception] {
        client.callRemote.unsafeRunSync()
      }.getMessage shouldBe s"POST $fusekiBaseUrl/${rdfStoreConfig.datasetName}/sparql returned $BadRequest; body: some message"
    }

    "fail if remote responds with OK status but non-expected body" in new QueryClientTestCase {

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .willReturn(okJson("abc"))
      }

      intercept[Exception] {
        client.callRemote.unsafeRunSync()
      }.getMessage should startWith(
        s"POST $fusekiBaseUrl/${rdfStoreConfig.datasetName}/sparql returned ${Status.Ok}; error: "
      )
    }
  }

  "send sparql update" should {

    "succeed returning unit if the update query succeeds" in new UpdateClientTestCase {

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/update")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
          .withRequestBody(equalTo(s"update=${client.query}"))
          .willReturn(ok())
      }

      client.sendUpdate.unsafeRunSync() shouldBe ((): Unit)
    }

    "fail if remote responds with non-OK status" in new UpdateClientTestCase {

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/update")
          .willReturn(
            aResponse
              .withStatus(BadRequest.code)
              .withBody("some message")
          )
      }

      intercept[Exception] {
        client.sendUpdate.unsafeRunSync()
      }.getMessage shouldBe s"POST $fusekiBaseUrl/${rdfStoreConfig.datasetName}/update returned $BadRequest; body: some message"
    }

    "use the given response mapping for calculating the result" in new UpdateClientTestCase {

      val responseMapper: PartialFunction[(Status, Request[IO], Response[IO]), IO[Any]] = {
        case (Ok, _, _)         => IO.unit
        case (BadRequest, _, _) => IO.pure("error")
      }

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/update")
          .willReturn(
            aResponse.withStatus(Ok.code)
          )
      }

      client.sendUpdate(responseMapper).unsafeRunSync() shouldBe ((): Unit)

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/update")
          .willReturn(
            aResponse.withStatus(BadRequest.code)
          )
      }

      client.sendUpdate(responseMapper).unsafeRunSync() shouldBe "error"
    }
  }

  private trait TestCase {
    val fusekiBaseUrl  = FusekiBaseUrl(externalServiceBaseUrl)
    val rdfStoreConfig = rdfStoreConfigs.generateOne.copy(fusekiBaseUrl = fusekiBaseUrl)
  }

  private trait QueryClientTestCase extends TestCase {
    val client = new TestRdfQueryClient(
      query = """SELECT ?s ?p ?o WHERE { ?s ?p ?o}""",
      rdfStoreConfig
    )
  }

  private trait UpdateClientTestCase extends TestCase {
    val client = new TestRdfClient(
      query = """INSERT { "o" "p" "s"} {}""",
      rdfStoreConfig
    )
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private class TestRdfClient(
      val query:      String,
      rdfStoreConfig: RdfStoreConfig
  ) extends IORdfStoreClient(rdfStoreConfig, TestLogger[IO]()) {

    def sendUpdate: IO[Unit] = updateWitNoResult(query)

    def sendUpdate[ResultType](
        mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[ResultType]]
    ): IO[ResultType] = updateWitMapping(query, mapResponse)
  }

  private class TestRdfQueryClient(val query: String, rdfStoreConfig: RdfStoreConfig)
      extends IORdfStoreClient(rdfStoreConfig, TestLogger[IO]()) {

    def callRemote: IO[Json] = queryExpecting[Json](query)
  }
}
