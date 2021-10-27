/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.graph.tokenrepository

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.syntax.EncoderOps
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.http.client.AccessToken
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class AccessTokenFinderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with MockFactory
    with should.Matchers {

  import AccessTokenFinder._

  "findAccessToken(ProjectId)" should {

    "return Some Personal Access Token if remote responds with OK and valid body" in new TestCase {

      val projectId = projectIds.generateOne
      val accessToken: AccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/projects/$projectId/tokens")
          .willReturn(okJson(accessToken.asJson.noSpaces))
      }

      accessTokenFinder.findAccessToken(projectId).unsafeRunSync() shouldBe Some(accessToken)
    }

    "return Some OAuth Access Token if remote responds with OK and valid body" in new TestCase {

      val projectId = projectIds.generateOne
      val accessToken: AccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/projects/$projectId/tokens")
          .willReturn(okJson(accessToken.asJson.noSpaces))
      }

      accessTokenFinder.findAccessToken(projectId).unsafeRunSync() shouldBe Some(accessToken)
    }

    "return None if remote responds with NOT_FOUND" in new TestCase {

      val projectId = projectIds.generateOne

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

      val projectId = projectIds.generateOne

      stubFor {
        get(s"/projects/$projectId/tokens")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        accessTokenFinder.findAccessToken(projectId).unsafeRunSync()
      }.getMessage shouldBe s"GET $tokenRepositoryUrl/projects/$projectId/tokens returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}; Access token cannot be deserialized"
    }
  }

  "findAccessToken(ProjectPath)" should {

    "return Some Personal Access Token if remote responds with OK and valid body" in new TestCase {

      val projectPath = projectPaths.generateOne
      val accessToken: AccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/projects/${urlEncode(projectPath.toString)}/tokens")
          .willReturn(okJson(accessToken.asJson.noSpaces))
      }

      accessTokenFinder.findAccessToken(projectPath).unsafeRunSync() shouldBe Some(accessToken)
    }

    "return Some OAuth Access Token if remote responds with OK and valid body" in new TestCase {

      val projectPath = projectPaths.generateOne
      val accessToken: AccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/projects/${urlEncode(projectPath.toString)}/tokens")
          .willReturn(okJson(accessToken.asJson.noSpaces))
      }

      accessTokenFinder.findAccessToken(projectPath).unsafeRunSync() shouldBe Some(accessToken)
    }

    "return None if remote responds with NOT_FOUND" in new TestCase {

      val projectPath = projectPaths.generateOne

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

      stubFor {
        get(s"/projects/${urlEncode(projectPath.toString)}/tokens")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        accessTokenFinder.findAccessToken(projectPath).unsafeRunSync()
      }.getMessage shouldBe s"GET $tokenRepositoryUrl/projects/${urlEncode(projectPath.toString)}/tokens returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}; Access token cannot be deserialized"
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger()
    val tokenRepositoryUrl = TokenRepositoryUrl(externalServiceBaseUrl)
    val accessTokenFinder  = new AccessTokenFinderImpl[IO](tokenRepositoryUrl)
  }
}
