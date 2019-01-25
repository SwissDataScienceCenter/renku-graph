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

package ch.datascience.webhookservice.hookcreation

import cats.effect.{IO, Sync}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.graph.events.ProjectPath
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.config.GitLabConfig.HostUrl
import ch.datascience.webhookservice.config.IOGitLabConfigProvider
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.Url
import io.circe.Json
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOHookAccessTokenVerifierSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "checkHookAccessTokenPresence" should {

    "return true if a '<projectPath-hook-access-token>' token was created - personal access token case" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/users/${projectInfo.owner.id}/impersonation_tokens")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .willReturn(okJson(withTokens(oneTokenFor = projectInfo.path)))
      }

      verifier.checkHookAccessTokenPresence(projectInfo, personalAccessToken).unsafeRunSync() shouldBe true
    }

    "return true if a 'projectPath-hook-access-token' token was created - oauth token case" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/users/${projectInfo.owner.id}/impersonation_tokens")
          .withHeader("Authorization", equalTo(s"Bearer $oauthAccessToken"))
          .willReturn(okJson(withTokens(oneTokenFor = projectInfo.path)))
      }

      verifier.checkHookAccessTokenPresence(projectInfo, oauthAccessToken).unsafeRunSync() shouldBe true
    }

    "return false if a 'projectPath-hook-access-token' token does not exist" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      val otherProjectPath = projectPaths generateDifferentThan projectInfo.path
      stubFor {
        get(s"/api/v4/users/${projectInfo.owner.id}/impersonation_tokens")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .willReturn(okJson(withTokens(oneTokenFor = otherProjectPath)))
      }

      verifier.checkHookAccessTokenPresence(projectInfo, personalAccessToken).unsafeRunSync() shouldBe false
    }

    "fail if fetching the the config fails" in new TestCase {
      val personalAccessToken = personalAccessTokens.generateOne
      val exception           = exceptions.generateOne

      expectGitLabConfigProvider(returning = IO.raiseError(exception))

      intercept[Exception] {
        verifier.checkHookAccessTokenPresence(projectInfo, personalAccessToken).unsafeRunSync()
      } shouldBe exception
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/users/${projectInfo.owner.id}/impersonation_tokens")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        verifier.checkHookAccessTokenPresence(projectInfo, personalAccessToken).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return a RuntimeException if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/users/${projectInfo.owner.id}/impersonation_tokens")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .willReturn(serviceUnavailable().withBody("some error"))
      }

      intercept[Exception] {
        verifier.checkHookAccessTokenPresence(projectInfo, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/users/${projectInfo.owner.id}/impersonation_tokens returned ${Status.ServiceUnavailable}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/users/${projectInfo.owner.id}/impersonation_tokens")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        verifier.checkHookAccessTokenPresence(projectInfo, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/users/${projectInfo.owner.id}/impersonation_tokens returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
    }
  }

  private trait TestCase {
    val gitLabUrl   = url(externalServiceBaseUrl)
    val projectInfo = projectInfos.generateOne

    val configProvider = mock[IOGitLabConfigProvider]

    def expectGitLabConfigProvider(returning: IO[HostUrl]) =
      (configProvider
        .get()(_: Sync[IO]))
        .expects(*)
        .returning(returning)

    val verifier = new IOHookAccessTokenVerifier(configProvider)
  }

  private def withTokens(oneTokenFor: ProjectPath): String =
    Json
      .arr(
        Json.obj(
          "active" -> Json.fromBoolean(true),
          "scopes" -> Json.arr(Json.fromString("api")),
          "name"   -> Json.fromString("other-token-1")
        ),
        Json.obj(
          "active" -> Json.fromBoolean(true),
          "scopes" -> Json.arr(Json.fromString("api")),
          "name"   -> Json.fromString(s"${oneTokenFor.value.replace("/", "-")}-hook-access-token")
        ),
        Json.obj(
          "active"     -> Json.fromBoolean(true),
          "scopes"     -> Json.arr(Json.fromString("api")),
          "name"       -> Json.fromString("other-token-2"),
          "expires_at" -> Json.fromString("2017-04-04")
        )
      )
      .toString()

  private def url(value: String) =
    RefType
      .applyRef[String Refined Url](value)
      .getOrElse(throw new IllegalArgumentException("Invalid url value"))
}
