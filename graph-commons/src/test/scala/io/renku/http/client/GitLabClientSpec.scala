package io.renku.http.client

import cats.effect.IO
import cats.implicits.{catsSyntaxApplicativeErrorId, toShow}
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.MappingBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.{oauthAccessTokens, pages, personalAccessTokens}
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
import org.http4s.Status.{NotFound, Ok, Unauthorized}
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.{Method, Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GitLabClientSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with MockFactory {

  "send" should {

    "fetch json from a given page" in new TestCase {

      stubFor {
        callMethod(method, s"api/v4/$path")
          .willReturn(
            okJson(result)
              .withHeader("X-Next-Page", maybeNextPage.map(_.show).getOrElse(""))
          )
      }

      client.send(method, path, endpointName)(_ => IO(result)).unsafeRunSync() shouldBe result
    }

    "fetch commits using the given personal access token" in new TestCase {
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        callMethod(method, s"api/v4/$path")
          .withAccessToken(personalAccessToken.some)
          .willReturn(
            okJson(result)
              .withHeader("X-Next-Page", maybeNextPage.map(_.show).getOrElse(""))
          )
      }

      client.send(method, path, endpointName)(_ => IO(result)).unsafeRunSync() shouldBe result
    }

    "fetch commits using the given oauth token" in new TestCase {
      val authAccessToken = oauthAccessTokens.generateOne

      stubFor {
        callMethod(method, s"api/v4/$path")
          .withHeader("PRIVATE-TOKEN", equalTo(authAccessToken.value))
          .willReturn(
            okJson(result)
              .withHeader("X-Next-Page", maybeNextPage.map(_.show).getOrElse(""))
          )
      }

      client.send(method, path, endpointName)(_ => IO(result)).unsafeRunSync() shouldBe result
    }

    "Handle empty JSON" in new TestCase {
      override val result: String = "{}"
      stubFor {
        callMethod(method, s"api/v4/$path")
          .willReturn(
            okJson(result)
              .withHeader("X-Next-Page", maybeNextPage.map(_.show).getOrElse(""))
          )
      }

      client.send(method, path, endpointName)(_ => IO(result)).unsafeRunSync() shouldBe result

    }

    "Handle 404 NOT FOUND without throwing" in new TestCase {
      stubFor {
        callMethod(method, s"api/v4/$path")
          .willReturn(
            notFound()
          )
      }

      client.send(method, path, endpointName)(_ => IO(())).unsafeRunSync() shouldBe ()

    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      stubFor {
        callMethod(method, s"/api/v4/$path")
          .willReturn(
            unauthorized()
          )
      }

      private implicit lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[Int]]] = {
        case (Ok, _, response)    => response.as[List[Int]].map(_.headOption)
        case (NotFound, _, _)     => Option.empty[Int].pure[IO]
        case (Unauthorized, _, _) => UnauthorizedException.raiseError[IO, Option[Int]]
      }

      intercept[Exception] {
        client.send(method, path, endpointName)(mapResponse).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {
      stubFor {
        callMethod(method, s"/api/v4/$path")
          .willReturn(
            badRequest()
          )
      }

      private implicit lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[Int]]] = {
        case (Ok, _, response)    => response.as[List[Int]].map(_.headOption)
        case (NotFound, _, _)     => Option.empty[Int].pure[IO]
        case (Unauthorized, _, _) => UnauthorizedException.raiseError[IO, Option[Int]]
      }

      intercept[Exception](client.send(method, path, endpointName)(mapResponse).unsafeRunSync())
    }

    "return an Exception if remote client responds with unexpected body" in new TestCase {
      stubFor {
        callMethod(method, s"/api/v4/$path")
          .willReturn(okJson("{}"))
      }

      private implicit lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[Int]]] = {
        case (Ok, _, response)    => response.as[List[Int]].map(_.headOption)
        case (NotFound, _, _)     => Option.empty[Int].pure[IO]
        case (Unauthorized, _, _) => UnauthorizedException.raiseError[IO, Option[Int]]
      }

      intercept[Exception] {
        client.send(method, path, endpointName)(mapResponse).unsafeRunSync()
      }.getMessage should startWith(
        s"$method $gitLabApiUrl/$path returned 200 OK; error: Invalid message body: Could not decode JSON: {}; C[A]"
      )
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateSome

    private implicit val logger: TestLogger[IO] = TestLogger()
    val apiCallRecorder = new GitLabApiCallRecorder(TestExecutionTimeRecorder[IO]())
    val gitLabApiUrl    = GitLabUrl(externalServiceBaseUrl).apiV4
    val client          = new GitLabClientImpl[IO](gitLabApiUrl, apiCallRecorder, Throttler.noThrottling)

    val method       = Gen.oneOf(GET, PUT, DELETE, POST).generateOne
    val endpointName = nonBlankStrings().generateOne
    val queryParams =
      nonBlankStrings().generateList().map(key => (key.value, nonBlankStrings().generateOne.value)).toMap
    val path = Uri
      .fromString(nonBlankStrings().generateNonEmptyList().toList.mkString("/"))
      .fold(throw _, identity)
      .withQueryParams(queryParams)

    val maybeNextPage = pages.generateOption

    val result = jsons.generateOne.toString()

  }

  private def callMethod(method: Method, uri: String): MappingBuilder = method match {
    case GET    => get(uri)
    case PUT    => put(uri)
    case DELETE => delete(uri)
    case POST   => post(uri)
  }
}
