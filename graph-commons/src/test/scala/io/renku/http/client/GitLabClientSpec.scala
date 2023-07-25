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

package io.renku.http.client

import cats.effect.IO
import cats.implicits.{catsSyntaxApplicativeErrorId, toShow}
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.MappingBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Json
import io.circe.syntax._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GitLabUrl
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.metrics.GitLabApiCallRecorder
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Method.{GET, _}
import org.http4s.Status.{Accepted, NotFound, Ok, Unauthorized}
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.{Header, Method, Request, Response, Status, Uri, UrlForm}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.ci._

import scala.util.Try

class GitLabClientSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with TryValues
    with TableDrivenPropertyChecks
    with MockFactory {

  "get" should {

    val mapGetResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[List[String]]] = {
      case (Ok, _, response)    => response.as[List[String]]
      case (NotFound, _, _)     => List.empty[String].pure[IO]
      case (Unauthorized, _, _) => UnauthorizedException.raiseError[IO, List[String]]
    }

    "fetch json from a given page" in new TestCase {

      stubFor {
        callMethod(GET, s"/api/v4/$path")
          .willReturn(
            okJson(result.asJson.toString())
              .withHeader("X-Next-Page", maybeNextPage.map(_.show).getOrElse(""))
          )
      }

      client.get(path, endpointName)(mapGetResponse).unsafeRunSync() shouldBe result
    }

    forAll(tokenScenarios) { (tokenType, accessToken: AccessToken) =>
      s"use the given $tokenType and call mapResponse" in new TestCase {

        val returnValue = Gen
          .oneOf(
            notFound()                    -> List.empty[Json],
            okJson(result.asJson.spaces2) -> result
          )
          .generateOne

        stubFor {
          callMethod(GET, s"/api/v4/$path")
            .withAccessToken(accessToken.some)
            .willReturn(returnValue._1)
        }

        client.get(path, endpointName)(mapGetResponse)(accessToken.some).unsafeRunSync() shouldBe returnValue._2
      }
    }

    "handle 404 NOT FOUND without throwing" in new TestCase {
      stubFor {
        callMethod(GET, s"/api/v4/$path")
          .willReturn(notFound())
      }

      client.get(path, endpointName)(mapGetResponse).unsafeRunSync() shouldBe List()
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      stubFor {
        callMethod(GET, s"/api/v4/$path")
          .willReturn(unauthorized())
      }

      intercept[UnauthorizedException] {
        client.get(path, endpointName)(mapGetResponse).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {
      stubFor {
        callMethod(GET, s"/api/v4/$path")
          .willReturn(badRequest())
      }

      intercept[Exception](client.get(path, endpointName)(mapGetResponse).unsafeRunSync())
    }

    "return an Exception if remote client responds with unexpected body" in new TestCase {
      stubFor {
        callMethod(GET, s"/api/v4/$path")
          .willReturn(okJson("not json"))
      }

      intercept[Exception] {
        client.get(path, endpointName)(mapGetResponse).unsafeRunSync()
      }.getMessage should startWith(
        s"$GET $gitLabApiUrl/$path returned 200 OK; error: Malformed message body: Invalid JSON"
      )
    }
  }

  "head" should {

    val mapHeadResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Boolean]] = { case (Ok, _, _) =>
      true.pure[IO]
    }

    "send HEAD request to the given URL and apply the given mapping" in new TestCase {

      stubFor {
        callMethod(HEAD, s"/api/v4/$path")
          .willReturn(ok())
      }

      client.head(path, endpointName)(mapHeadResponse).unsafeRunSync() shouldBe true
    }

    forAll(tokenScenarios) { (tokenType, accessToken: AccessToken) =>
      s"use the given $tokenType and call mapResponse" in new TestCase {

        stubFor {
          callMethod(HEAD, s"/api/v4/$path")
            .withAccessToken(accessToken.some)
            .willReturn(ok())
        }

        client.head(path, endpointName)(mapHeadResponse)(accessToken.some).unsafeRunSync() shouldBe true
      }
    }

    "return an Exception if remote responds with status different than expected in mapping" in new TestCase {

      stubFor {
        callMethod(HEAD, s"/api/v4/$path")
          .willReturn(noContent())
      }

      val exception = intercept[Exception](client.head(path, endpointName)(mapHeadResponse).unsafeRunSync())

      exception shouldBe a[RestClientError.UnexpectedResponseException]
    }
  }

  "post" should {

    val mapPostResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
      case (Accepted, _, _)     => ().pure[IO]
      case (Unauthorized, _, _) => UnauthorizedException.raiseError[IO, Unit]
    }

    forAll(tokenScenarios) { (tokenType, accessToken: AccessToken) =>
      s"send json with the $tokenType to the endpoint" in new TestCase {

        val payload = jsons.generateOne
        stubFor {
          post(s"/api/v4/$path")
            .withAccessToken(accessToken.some)
            .withRequestBody(equalToJson(payload.spaces2))
            .willReturn(aResponse().withStatus(Accepted.code))
        }

        client.post(path, endpointName, payload)(mapPostResponse)(accessToken.some).unsafeRunSync() shouldBe ()
      }
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {

      val payload = jsons.generateOne

      stubFor {
        post(s"/api/v4/$path")
          .withAccessToken(maybeAccessToken)
          .withRequestBody(equalToJson(payload.spaces2))
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        client.post(path, endpointName, payload)(mapPostResponse).unsafeRunSync()
      } shouldBe UnauthorizedException
    }
  }

  "put" should {

    val mapPutResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
      case (Accepted, _, _)     => ().pure[IO]
      case (Unauthorized, _, _) => UnauthorizedException.raiseError[IO, Unit]
    }

    forAll(tokenScenarios) { (tokenType, accessToken: AccessToken) =>
      s"send form data with the $tokenType to the endpoint" in new TestCase {

        val propName  = nonEmptyStrings().generateOne
        val propValue = nonEmptyStrings().generateOne

        stubFor {
          put(s"/api/v4/$path")
            .withAccessToken(accessToken.some)
            .withRequestBody(equalTo(s"$propName=$propValue"))
            .willReturn(aResponse().withStatus(Accepted.code))
        }

        client
          .put(path, endpointName, UrlForm(propName -> propValue))(mapPutResponse)(accessToken.some)
          .unsafeRunSync() shouldBe ()
      }
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {

      val propName  = nonEmptyStrings().generateOne
      val propValue = nonEmptyStrings().generateOne

      stubFor {
        put(s"/api/v4/$path")
          .withAccessToken(maybeAccessToken)
          .withRequestBody(equalTo(s"$propName=$propValue"))
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        client.put(path, endpointName, UrlForm(propName -> propValue))(mapPutResponse).unsafeRunSync()
      } shouldBe UnauthorizedException
    }
  }

  "delete" should {

    val mapDeleteResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Unit]] = {
      case (Accepted, _, _)     => ().pure[IO]
      case (Unauthorized, _, _) => UnauthorizedException.raiseError[IO, Unit]
    }

    forAll(tokenScenarios) { (tokenType, accessToken: AccessToken) =>
      s"send json with the $tokenType to the endpoint" in new TestCase {

        stubFor {
          delete(s"/api/v4/$path")
            .withAccessToken(accessToken.some)
            .willReturn(aResponse().withStatus(Accepted.code))
        }

        client.delete(path, endpointName)(mapDeleteResponse)(accessToken.some).unsafeRunSync() shouldBe ()
      }
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {

      stubFor {
        delete(s"/api/v4/$path")
          .willReturn(unauthorized())
      }

      intercept[UnauthorizedException] {
        client.delete(path, endpointName)(mapDeleteResponse).unsafeRunSync()
      } shouldBe UnauthorizedException
    }
  }

  "maybeNextPage" should {

    "return the value from the 'X-Next-Page' response header" in {

      val page     = pages.generateOne
      val response = Response[Try]().withHeaders(Header.Raw(ci"X-Next-Page", page.value.show))

      GitLabClient.maybeNextPage[Try](response).success.value shouldBe page.some
    }

    "return None if no 'X-Next-Page' header in the response" in {
      GitLabClient.maybeNextPage[Try](Response[Try]()).success.value shouldBe None
    }
  }

  "maybeTotalPages" should {

    "return the value from the 'X-Total-Pages' response header" in {

      val total    = totals.generateOne
      val response = Response[Try]().withHeaders(Header.Raw(ci"X-Total-Pages", total.value.show))

      GitLabClient.maybeTotalPages[Try](response).success.value shouldBe total.some
    }

    "return None if no 'X-Total-Pages' header in the response" in {
      GitLabClient.maybeTotalPages[Try](Response[Try]()).success.value shouldBe None
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateSome

    private implicit val logger: TestLogger[IO] = TestLogger()
    val apiCallRecorder = new GitLabApiCallRecorder(TestExecutionTimeRecorder[IO]())
    val gitLabApiUrl    = GitLabUrl(externalServiceBaseUrl).apiV4
    val client          = new GitLabClientImpl[IO](gitLabApiUrl, apiCallRecorder, Throttler.noThrottling)

    val endpointName = nonBlankStrings().generateOne
    val queryParams =
      nonBlankStrings().generateList().map(key => (key.value, nonBlankStrings().generateOne.value)).toMap
    val path = Uri
      .fromString(nonBlankStrings().generateNonEmptyList().toList.mkString("/"))
      .fold(throw _, identity)
      .withQueryParams(queryParams)

    val maybeNextPage = pages.generateOption

    val result = nonEmptyStringsList(5, 10).generateOne
  }

  private def callMethod(method: Method, uri: String): MappingBuilder = method match {
    case GET    => get(uri)
    case HEAD   => head(urlEqualTo(uri))
    case PUT    => put(uri)
    case DELETE => delete(uri)
    case POST   => post(uri)
  }

  private lazy val tokenScenarios = Table(
    "token type"              -> "token",
    "Project Access Token"    -> projectAccessTokens.generateOne,
    "User OAuth Access Token" -> userOAuthAccessTokens.generateOne,
    "Personal Access Token"   -> personalAccessTokens.generateOne
  )
}
