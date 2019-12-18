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

package ch.datascience.graph.tokenrepository

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.http.client.UrlEncoder.urlEncode
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class AccessTokenFinderSpec extends WordSpec with ExternalServiceStubbing with MockFactory {

  import IOAccessTokenFinder._

  "findAccessToken(ProjectId)" should {

    "return Some Personal Access Token if remote responds with OK and valid body" in new TestCase {

      val projectId   = projectIds.generateOne
      val accessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/projects/$projectId/tokens")
          .willReturn(okJson(s"""{"personalAccessToken": "${accessToken.value}"}"""))
      }

      accessTokenFinder.findAccessToken(projectId).unsafeRunSync() shouldBe Some(accessToken)
    }

    "return Some OAuth Access Token if remote responds with OK and valid body" in new TestCase {

      val projectId   = projectIds.generateOne
      val accessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/projects/$projectId/tokens")
          .willReturn(okJson(s"""{"oauthAccessToken": "${accessToken.value}"}"""))
      }

      accessTokenFinder.findAccessToken(projectId).unsafeRunSync() shouldBe Some(accessToken)
    }

    "return None if remote responds with NOT_FOUND" in new TestCase {

      val projectId   = projectIds.generateOne
      val accessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/projects/$projectId/tokens")
          .willReturn(notFound())
      }

      accessTokenFinder.findAccessToken(projectId).unsafeRunSync() shouldBe None
    }

    "return a RuntimeException if remote responds with status neither OK nor NOT_FOUND" in new TestCase {

      val projectId = projectIds.generateOne

      stubFor {
        get(s"/projects/$projectId/tokens")
          .willReturn(
            aResponse
              .withStatus(Status.BadGateway.code)
              .withHeader("content-type", "application/json")
              .withBody("some body")
          )
      }

      intercept[Exception] {
        accessTokenFinder.findAccessToken(projectId).unsafeRunSync()
      }.getMessage shouldBe s"GET $tokenRepositoryUrl/projects/$projectId/tokens returned ${Status.BadGateway}; body: some body"
    }

    "return a RuntimeException if remote responds with unexpected body" in new TestCase {

      val projectId   = projectIds.generateOne
      val accessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/projects/$projectId/tokens")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        accessTokenFinder.findAccessToken(projectId).unsafeRunSync()
      }.getMessage shouldBe s"GET $tokenRepositoryUrl/projects/$projectId/tokens returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
    }
  }

  "findAccessToken(ProjectPath)" should {

    "return Some Personal Access Token if remote responds with OK and valid body" in new TestCase {

      val projectPath = projectPaths.generateOne
      val accessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/projects/${urlEncode(projectPath.toString)}/tokens")
          .willReturn(okJson(s"""{"personalAccessToken": "${accessToken.value}"}"""))
      }

      accessTokenFinder.findAccessToken(projectPath).unsafeRunSync() shouldBe Some(accessToken)
    }

    "return Some OAuth Access Token if remote responds with OK and valid body" in new TestCase {

      val projectPath = projectPaths.generateOne
      val accessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/projects/${urlEncode(projectPath.toString)}/tokens")
          .willReturn(okJson(s"""{"oauthAccessToken": "${accessToken.value}"}"""))
      }

      accessTokenFinder.findAccessToken(projectPath).unsafeRunSync() shouldBe Some(accessToken)
    }

    "return None if remote responds with NOT_FOUND" in new TestCase {

      val projectPath = projectPaths.generateOne
      val accessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/projects/${urlEncode(projectPath.toString)}/tokens")
          .willReturn(notFound())
      }

      accessTokenFinder.findAccessToken(projectPath).unsafeRunSync() shouldBe None
    }

    "return a RuntimeException if remote responds with status neither OK nor NOT_FOUND" in new TestCase {

      val projectPath = projectPaths.generateOne

      stubFor {
        get(s"/projects/${urlEncode(projectPath.toString)}/tokens")
          .willReturn(
            aResponse
              .withStatus(Status.BadGateway.code)
              .withHeader("content-type", "application/json")
              .withBody("some body")
          )
      }

      intercept[Exception] {
        accessTokenFinder.findAccessToken(projectPath).unsafeRunSync()
      }.getMessage shouldBe s"GET $tokenRepositoryUrl/projects/${urlEncode(projectPath.toString)}/tokens returned ${Status.BadGateway}; body: some body"
    }

    "return a RuntimeException if remote responds with unexpected body" in new TestCase {

      val projectPath = projectPaths.generateOne
      val accessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/projects/${urlEncode(projectPath.toString)}/tokens")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        accessTokenFinder.findAccessToken(projectPath).unsafeRunSync()
      }.getMessage shouldBe s"GET $tokenRepositoryUrl/projects/${urlEncode(projectPath.toString)}/tokens returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val tokenRepositoryUrl = TokenRepositoryUrl(externalServiceBaseUrl)

    val accessTokenFinder = new IOAccessTokenFinder(tokenRepositoryUrl, TestLogger())
  }
}
