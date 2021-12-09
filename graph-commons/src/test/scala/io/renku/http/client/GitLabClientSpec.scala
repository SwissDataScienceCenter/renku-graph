package io.renku.http.client

import cats.effect.IO
import cats.implicits.{catsSyntaxApplicativeErrorId, toShow}
import com.github.tomakehurst.wiremock.client.MappingBuilder
import com.github.tomakehurst.wiremock.client.WireMock._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.{oauthAccessTokens, pages, personalAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GitLabUrl
import io.renku.http.client.RestClientError.{UnauthorizedException, UnexpectedResponseException}
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.metrics.GitLabApiCallRecorder
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Method._
import org.http4s.Status.InternalServerError
import org.http4s.syntax.all._
import org.http4s.{Method, Uri}
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
        callMethod(method, uri)
          .willReturn(
            okJson(result)
              .withHeader("X-Next-Page", maybeNextPage.map(_.show).getOrElse(""))
          )
      }

      client.send(method, uri, endpointName)(_ => IO(result)).unsafeRunSync() shouldBe result
    }

    "fetch commits using the given personal access token" in new TestCase {
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        callMethod(method, uri)
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(
            okJson(result)
              .withHeader("X-Next-Page", maybeNextPage.map(_.show).getOrElse(""))
          )
      }

      client.send(method, uri, endpointName)(_ => IO(result)).unsafeRunSync() shouldBe result
    }

    "fetch commits using the given oauth token" in new TestCase {
      val authAccessToken = oauthAccessTokens.generateOne

      stubFor {
        callMethod(method, uri)
          .withHeader("PRIVATE-TOKEN", equalTo(authAccessToken.value))
          .willReturn(
            okJson(result)
              .withHeader("X-Next-Page", maybeNextPage.map(_.show).getOrElse(""))
          )
      }

      client.send(method, uri, endpointName)(_ => IO(result)).unsafeRunSync() shouldBe result
    }

    "Handle empty JSON" in new TestCase {
      override val result: String = "{}"
      stubFor {
        callMethod(method, uri)
          .willReturn(
            okJson(result)
              .withHeader("X-Next-Page", maybeNextPage.map(_.show).getOrElse(""))
          )
      }

      client.send(method, uri, endpointName)(_ => IO(result)).unsafeRunSync() shouldBe result

    }

    "Handle 404 NOT FOUND without throwing" in new TestCase {
      stubFor {
        callMethod(method, uri)
          .willReturn(
            notFound()
          )
      }

      client.send(method, uri, endpointName)(_ => IO(())).unsafeRunSync() shouldBe ()

    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      stubFor {
        callMethod(method, uri)
          .willReturn(
            unauthorized()
          )
      }

      intercept[Exception] {
        client.send(method, uri, endpointName)(_ => UnauthorizedException.raiseError[IO, Unit]).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {
      stubFor {
        callMethod(method, uri)
          .willReturn(
            badRequest()
          )
      }

      intercept[Exception] {
        client
          .send(method, uri, endpointName)(_ =>
            UnexpectedResponseException(InternalServerError, "Unexpected response").raiseError[IO, Unit]
          )
          .unsafeRunSync()
      }
    }

    "return an Exception if remote client responds with unexpected body" in new TestCase {
      stubFor {
        callMethod(method, uri)
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        client
          .send(method, uri, endpointName)(_ => new Exception("Unexpected body").raiseError[IO, Unit])
          .unsafeRunSync()
      }.getMessage should startWith(s"$method $externalServiceBaseUrl$uri returned 200 OK; error: Unexpected body")
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateSome

    private implicit val logger: TestLogger[IO] = TestLogger()
    val apiCallRecorder = new GitLabApiCallRecorder(TestExecutionTimeRecorder[IO]())
    val gitLabApiUrl    = GitLabUrl(externalServiceBaseUrl).apiV4
    val client          = new GitLabClientImpl[IO](gitLabApiUrl, apiCallRecorder, Throttler.noThrottling)

    val method        = Gen.oneOf(GET, PUT, DELETE, POST).generateOne
    val endpointName  = nonBlankStrings().generateOne
    val uri           = uri"" / nonBlankStrings().generateList().mkString("/")
    val maybeNextPage = pages.generateOption

    val result = jsons.generateOne.toString()

  }

  private def callMethod(method: Method, uri: Uri): MappingBuilder = method match {
    case GET    => get(uri.show)
    case PUT    => put(uri.show)
    case DELETE => delete(uri.show)
    case POST   => post(uri.show)
  }
}
