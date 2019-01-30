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

import java.net.URLEncoder

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.config.GitLabConfig.HostUrl
import ch.datascience.webhookservice.config.IOGitLabConfigProvider
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import com.github.tomakehurst.wiremock.client.WireMock._
import com.google.common.net.HttpHeaders.CONTENT_TYPE
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.Url
import io.circe.Json
import org.http4s.Status
import org.http4s.Status.Created
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOHookAccessTokenCreatorSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "createHookAccessToken" should {

    "return created Hook Access Token if a '<projectPath-hook-access-token>' token was created - personal access token case" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        post(s"/api/v4/users/${projectInfo.owner.id}/impersonation_tokens")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .withRequestBody(containing(s"name=${projectInfo.path.value.replace("/", "-")}-hook-access-token"))
          .withRequestBody(containing(s"${URLEncoder.encode("scopes[]", "UTF-8")}=api"))
          .withRequestBody(containing(s"user_id=${projectInfo.owner.id}"))
          .willReturn(createdJson(tokenCreationResponse))
      }

      accessTokenCreator
        .createHookAccessToken(projectInfo, personalAccessToken)
        .unsafeRunSync() shouldBe hookAccessToken
    }

    "return created Hook Access Token if a '<projectPath-hook-access-token>' token was created - oauth token case" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        post(s"/api/v4/users/${projectInfo.owner.id}/impersonation_tokens")
          .withHeader("Authorization", equalTo(s"Bearer $oauthAccessToken"))
          .withRequestBody(containing(s"name=${projectInfo.path.value.replace("/", "-")}-hook-access-token"))
          .withRequestBody(containing(s"${URLEncoder.encode("scopes[]", "UTF-8")}=api"))
          .withRequestBody(containing(s"user_id=${projectInfo.owner.id}"))
          .willReturn(createdJson(tokenCreationResponse))
      }

      accessTokenCreator
        .createHookAccessToken(projectInfo, oauthAccessToken)
        .unsafeRunSync() shouldBe hookAccessToken
    }

    "fail if fetching the the config fails" in new TestCase {
      val personalAccessToken = personalAccessTokens.generateOne
      val exception           = exceptions.generateOne

      expectGitLabConfigProvider(returning = IO.raiseError(exception))

      intercept[Exception] {
        accessTokenCreator.createHookAccessToken(projectInfo, personalAccessToken).unsafeRunSync()
      } shouldBe exception
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        post(s"/api/v4/users/${projectInfo.owner.id}/impersonation_tokens")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .withRequestBody(containing(s"name=${projectInfo.path.value.replace("/", "-")}-hook-access-token"))
          .withRequestBody(containing(s"${URLEncoder.encode("scopes[]", "UTF-8")}=api"))
          .withRequestBody(containing(s"user_id=${projectInfo.owner.id}"))
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        accessTokenCreator.createHookAccessToken(projectInfo, personalAccessToken).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return a RuntimeException if remote client responds with status neither CREATED nor UNAUTHORIZED" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        post(s"/api/v4/users/${projectInfo.owner.id}/impersonation_tokens")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .withRequestBody(containing(s"name=${projectInfo.path.value.replace("/", "-")}-hook-access-token"))
          .withRequestBody(containing(s"${URLEncoder.encode("scopes[]", "UTF-8")}=api"))
          .withRequestBody(containing(s"user_id=${projectInfo.owner.id}"))
          .willReturn(notFound().withBody("some error"))
      }

      intercept[Exception] {
        accessTokenCreator.createHookAccessToken(projectInfo, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"POST $gitLabUrl/api/v4/users/${projectInfo.owner.id}/impersonation_tokens returned ${Status.NotFound}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {
      expectGitLabConfigProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        post(s"/api/v4/users/${projectInfo.owner.id}/impersonation_tokens")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.toString))
          .withRequestBody(containing(s"name=${projectInfo.path.value.replace("/", "-")}-hook-access-token"))
          .withRequestBody(containing(s"${URLEncoder.encode("scopes[]", "UTF-8")}=api"))
          .withRequestBody(containing(s"user_id=${projectInfo.owner.id}"))
          .willReturn(createdJson("{}"))
      }

      intercept[Exception] {
        accessTokenCreator.createHookAccessToken(projectInfo, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"POST $gitLabUrl/api/v4/users/${projectInfo.owner.id}/impersonation_tokens returned ${Status.Created}; error: Invalid message body: Could not decode JSON: {}"
    }
  }

  private trait TestCase {
    val gitLabUrl       = url(externalServiceBaseUrl)
    val projectInfo     = projectInfos.generateOne
    val hookAccessToken = hookAccessTokens.generateOne

    val configProvider = mock[IOGitLabConfigProvider]

    def expectGitLabConfigProvider(returning: IO[HostUrl]) =
      (configProvider.get _)
        .expects()
        .returning(returning)

    val accessTokenCreator = new IOHookAccessTokenCreator(configProvider)

    lazy val tokenCreationResponse: String = Json
      .obj(
        "id"    -> Json.fromInt(positiveInts().generateOne),
        "token" -> Json.fromString(hookAccessToken.value),
        "name"  -> Json.fromString(s"${projectInfo.path.value.replace("/", "-")}-hook-access-token")
      )
      .toString()
  }

  private def url(value: String) =
    RefType
      .applyRef[String Refined Url](value)
      .getOrElse(throw new IllegalArgumentException("Invalid url value"))

  private def createdJson(body: String) =
    aResponse
      .withStatus(Created.code)
      .withHeader(CONTENT_TYPE, "application/json")
      .withBody(body)
}
