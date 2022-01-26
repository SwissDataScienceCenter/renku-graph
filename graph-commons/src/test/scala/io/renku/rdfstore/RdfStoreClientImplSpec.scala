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
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.generators.jsonld.JsonLDGenerators._
import io.renku.http.client.RestClient
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.http.rest.paging.Paging.PagedResultsFinder
import io.renku.http.rest.paging._
import io.renku.http.rest.paging.model.{Page, PerPage, Total}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.JsonLD
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status.{BadRequest, Ok}
import org.http4s.{Request, Response, Status}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.log4cats.Logger

import scala.util.Try

class RdfStoreClientImplSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with MockFactory
    with should.Matchers {

  "RdfStoreClientImpl" should {
    "be a IORestClient" in new QueryClientTestCase {
      type IORdfStoreClientImpl = RdfStoreClientImpl[IO]
      client shouldBe a[IORdfStoreClientImpl]
      client shouldBe a[RestClient[IO, _]]
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
          .withRequestBody(equalTo(s"query=${urlEncode(client.query.toString)}"))
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

  "send sparql query with paging request" should {

    import io.circe.literal._

    "do a call for total even if not full page is returned" in new QueryClientTestCase {

      val items = nonEmptyList(nonBlankStrings(), maxElements = 1).generateOne.map(_.value).toList
      val responseBody =
        json"""{
          "results": {
            "bindings": $items
          }
        }"""

      val pagingRequest = PagingRequest(Page.first, PerPage(2))

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
          .withHeader("accept", equalTo("application/sparql-results+json"))
          .withRequestBody(equalTo(s"query=${urlEncode(client.query.include[Try](pagingRequest).get.toString)}"))
          .willReturn(okJson(responseBody.noSpaces))
      }

      val totalResponseBody = json"""{
        "results": {
          "bindings": [
            {
              "total": {
                "value": ${items.size}
              }
            }
          ]
        }
      }"""
      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
          .withHeader("accept", equalTo("application/sparql-results+json"))
          .withRequestBody(equalTo(s"query=${urlEncode(client.query.toCountQuery.toString)}"))
          .willReturn(okJson(totalResponseBody.noSpaces))
      }

      val results = client.callWith(pagingRequest).unsafeRunSync()

      results.results                  shouldBe items
      results.pagingInfo.pagingRequest shouldBe pagingRequest
      results.pagingInfo.total         shouldBe Total(items.size)
    }

    "do a call to for total if a full page is returned" in new QueryClientTestCase {

      val allItems      = nonEmptyList(nonBlankStrings(), minElements = 5).generateOne.map(_.value).toList
      val pagingRequest = PagingRequest(Page.first, PerPage(4))
      val pageItems     = allItems.take(pagingRequest.perPage.value)
      val responseBody = json"""{
        "results": {
          "bindings": $pageItems
        }
      }"""

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
          .withHeader("accept", equalTo("application/sparql-results+json"))
          .withRequestBody(equalTo(s"query=${urlEncode(client.query.include[Try](pagingRequest).get.toString)}"))
          .willReturn(okJson(responseBody.noSpaces))
      }

      val totalResponseBody = json"""{
        "results": {
          "bindings": [
            {
              "total": {
                "value": ${allItems.size}
              }
            }
          ]
        }
      }"""
      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
          .withHeader("accept", equalTo("application/sparql-results+json"))
          .withRequestBody(equalTo(s"query=${urlEncode(client.query.toCountQuery.toString)}"))
          .willReturn(okJson(totalResponseBody.noSpaces))
      }

      val results = client.callWith(pagingRequest).unsafeRunSync()

      results.results                  shouldBe pageItems
      results.pagingInfo.pagingRequest shouldBe pagingRequest
      results.pagingInfo.total         shouldBe Total(allItems.size)
    }

    "use the special count query if given to fetch the total if a full page is returned" in new QueryClientTestCase {

      val allItems      = nonEmptyList(nonBlankStrings(), minElements = 5).generateOne.map(_.value).toList
      val pagingRequest = PagingRequest(Page.first, PerPage(4))
      val pageItems     = allItems.take(pagingRequest.perPage.value)
      val responseBody = json"""{
        "results": {
          "bindings": $pageItems
        }
      }"""

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
          .withHeader("accept", equalTo("application/sparql-results+json"))
          .withRequestBody(equalTo(s"query=${urlEncode(client.query.include[Try](pagingRequest).get.toString)}"))
          .willReturn(okJson(responseBody.noSpaces))
      }

      val countQuery = SparqlQuery(name = "test count query",
                                   prefixes = Set.empty,
                                   body = """SELECT ?s ?p ?o WHERE { ?s ?p ?o} ORDER BY ASC(?o)"""
      )
      val totalResponseBody = json"""{
        "results": {
          "bindings": [
            {
              "total": {
                "value": ${allItems.size}
              }
            }
          ]
        }
      }"""
      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
          .withHeader("accept", equalTo("application/sparql-results+json"))
          .withRequestBody(equalTo(s"query=${urlEncode(countQuery.toCountQuery.toString)}"))
          .willReturn(okJson(totalResponseBody.noSpaces))
      }

      val results = client.callWith(pagingRequest, maybeCountQuery = Some(countQuery)).unsafeRunSync()

      results.results                  shouldBe pageItems
      results.pagingInfo.pagingRequest shouldBe pagingRequest
      results.pagingInfo.total         shouldBe Total(allItems.size)
    }

    "fail if sparql body does not end with the ORDER BY clause" in new TestCase {

      val client = new TestRdfQueryClientImpl(
        query = SparqlQuery(name = "test query", Set.empty, "SELECT ?s ?p ?o WHERE { ?s ?p ?o}"),
        rdfStoreConfig
      )

      val exception = intercept[Exception] {
        client.callWith(pagingRequests.generateOne).unsafeRunSync()
      }

      exception.getMessage should include("ORDER BY")
    }

    "fail for problems with calling the storage" in new QueryClientTestCase {

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/sparql")
          .willReturn(
            aResponse
              .withStatus(BadRequest.code)
              .withBody("some message")
          )
      }

      intercept[Exception] {
        client.callWith(pagingRequests.generateOne).unsafeRunSync()
      }.getMessage shouldBe s"POST $fusekiBaseUrl/${rdfStoreConfig.datasetName}/sparql returned $BadRequest; body: some message"
    }
  }

  "send sparql update" should {

    "succeed returning unit if the update query succeeds" in new UpdateClientTestCase {

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/update")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/x-www-form-urlencoded"))
          .withRequestBody(equalTo(s"update=${urlEncode(client.query.toString)}"))
          .willReturn(ok())
      }

      client.sendUpdate.unsafeRunSync() shouldBe ()
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

  "upload json-ld" should {

    "store the given JsonLd object in the triples store" in new UpdateClientTestCase {
      val entity = jsonLDEntities.generateOne

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/data")
          .withBasicAuth(rdfStoreConfig.authCredentials.username.value, rdfStoreConfig.authCredentials.password.value)
          .withHeader("content-type", equalTo("application/ld+json"))
          .withRequestBody(equalToJson(entity.toJson.toString()))
          .willReturn(ok())
      }

      client.uploadJson(entity).unsafeRunSync() shouldBe ()
    }

    "fail if remote responds with non-OK status" in new UpdateClientTestCase {

      stubFor {
        post(s"/${rdfStoreConfig.datasetName}/data")
          .willReturn(
            aResponse
              .withStatus(BadRequest.code)
              .withBody("some message")
          )
      }

      intercept[Exception] {
        client.uploadJson(jsonLDEntities.generateOne).unsafeRunSync()
      }.getMessage shouldBe s"POST $fusekiBaseUrl/${rdfStoreConfig.datasetName}/data returned $BadRequest; body: some message"
    }
  }

  private trait TestCase {
    val fusekiBaseUrl  = FusekiBaseUrl(externalServiceBaseUrl)
    val rdfStoreConfig = rdfStoreConfigs.generateOne.copy(fusekiBaseUrl = fusekiBaseUrl)
  }

  private trait QueryClientTestCase extends TestCase {
    val client = new TestRdfQueryClientImpl(
      query = SparqlQuery(name = "find all triples",
                          prefixes = Set.empty,
                          body = """SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ASC(?s)"""
      ),
      rdfStoreConfig
    )
  }

  private trait UpdateClientTestCase extends TestCase {
    val client = new TestRdfClientImpl(
      query = SparqlQuery(name = "insert", Set.empty, """INSERT { 'o' 'p' 's' } {}"""),
      rdfStoreConfig
    )
  }

  private class TestRdfClientImpl(
      val query:      SparqlQuery,
      rdfStoreConfig: RdfStoreConfig
  )(implicit logger:  Logger[IO] = TestLogger[IO]())
      extends RdfStoreClientImpl(rdfStoreConfig, new SparqlQueryTimeRecorder(TestExecutionTimeRecorder[IO]())) {

    def sendUpdate: IO[Unit] = updateWithNoResult(query)

    def sendUpdate[ResultType](
        mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[ResultType]]
    ): IO[ResultType] = updateWitMapping(query, mapResponse)

    def uploadJson(json: JsonLD): IO[Unit] = upload(json)
  }

  private class TestRdfQueryClientImpl(val query: SparqlQuery, rdfStoreConfig: RdfStoreConfig)(implicit
      logger:                                     Logger[IO] = TestLogger[IO]()
  ) extends RdfStoreClientImpl(rdfStoreConfig, new SparqlQueryTimeRecorder(TestExecutionTimeRecorder[IO]()))
      with Paging[String] {

    def callRemote: IO[Json] = queryExpecting[Json](query)

    def callWith(pagingRequest:   PagingRequest,
                 maybeCountQuery: Option[SparqlQuery] = None
    ): IO[PagingResponse[String]] = {
      implicit val resultsFinder: PagedResultsFinder[IO, String] = pagedResultsFinder(query, maybeCountQuery)
      findPage[IO](pagingRequest)
    }
  }
}
