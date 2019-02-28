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

package ch.datascience.triplesgenerator.eventprocessing

import cats.effect.{ContextShift, IO}
import ch.datascience.config.ServiceUrl
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class AccessTokenFinderSpec extends WordSpec with ExternalServiceStubbing with MockFactory {

  "findAccessToken" should {

    "return Some Personal Access Token if remote responds with OK and valid body" in new TestCase {
      expectTokenRepositoryConfigProvider(returning = IO.pure(tokenRepositoryUrl))
      val accessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/projects/$projectId/tokens")
          .willReturn(okJson(s"""{"personalAccessToken": "${accessToken.value}"}"""))
      }

      accessTokenFinder.findAccessToken(projectId).unsafeRunSync() shouldBe Some(accessToken)
    }

    "return Some OAuth Access Token if remote responds with OK and valid body" in new TestCase {
      expectTokenRepositoryConfigProvider(returning = IO.pure(tokenRepositoryUrl))
      val accessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/projects/$projectId/tokens")
          .willReturn(okJson(s"""{"oauthAccessToken": "${accessToken.value}"}"""))
      }

      accessTokenFinder.findAccessToken(projectId).unsafeRunSync() shouldBe Some(accessToken)
    }

    "return None if remote responds with NOT_FOUND" in new TestCase {
      expectTokenRepositoryConfigProvider(returning = IO.pure(tokenRepositoryUrl))
      val accessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/projects/$projectId/tokens")
          .willReturn(notFound())
      }

      accessTokenFinder.findAccessToken(projectId).unsafeRunSync() shouldBe None
    }

    "return a RuntimeException if remote responds with status neither OK nor NOT_FOUND" in new TestCase {
      expectTokenRepositoryConfigProvider(returning = IO.pure(tokenRepositoryUrl))

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
      expectTokenRepositoryConfigProvider(returning = IO.pure(tokenRepositoryUrl))
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

  private implicit val cs: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    val tokenRepositoryUrl = ServiceUrl(externalServiceBaseUrl)
    val projectId          = projectIds.generateOne

    val configProvider = mock[IOTokenRepositoryUrlProvider]

    def expectTokenRepositoryConfigProvider(returning: IO[ServiceUrl]) =
      (configProvider.get _)
        .expects()
        .returning(returning)

    val accessTokenFinder = new IOAccessTokenFinder(configProvider)
  }
}
