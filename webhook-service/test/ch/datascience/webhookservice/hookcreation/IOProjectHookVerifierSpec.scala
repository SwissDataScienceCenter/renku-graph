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

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.GraphCommonsGenerators._
import ch.datascience.graph.events.ProjectId
import ch.datascience.stubbing.ExternalServiceStubbing
import ch.datascience.webhookservice.config.{GitLabConfig, IOGitLabConfigProvider}
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.hookcreation.HookCreationGenerators._
import ch.datascience.webhookservice.hookcreation.ProjectHookUrlFinder.ProjectHookUrl
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.Url
import io.circe.Json
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOProjectHookVerifierSpec extends WordSpec with MockFactory with ExternalServiceStubbing {

  "checkProjectHookPresence" should {

    "return true if there's a hook with url pointing to expected project hook url - personal access token case" in new TestCase {
      expectGitLabUrlProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson(withHooks(projectId, oneHookUrl = projectHookId.projectHookUrl)))
      }

      verifier.checkProjectHookPresence(projectHookId, personalAccessToken).unsafeRunSync() shouldBe true
    }

    "return true if there's a hook with url pointing to expected project hook url - oauth token case" in new TestCase {
      expectGitLabUrlProvider(returning = IO.pure(gitLabUrl))
      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("Authorization", equalTo(s"Bearer ${oauthAccessToken.value}"))
          .willReturn(okJson(withHooks(projectId, oneHookUrl = projectHookId.projectHookUrl)))
      }

      verifier.checkProjectHookPresence(projectHookId, oauthAccessToken).unsafeRunSync() shouldBe true
    }

    "return false if there's no hook with url pointing to expected project hook url" in new TestCase {
      expectGitLabUrlProvider(returning = IO.pure(gitLabUrl))
      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("Authorization", equalTo(s"Bearer ${oauthAccessToken.value}"))
          .willReturn(okJson(
            withHooks(projectId, oneHookUrl = projectHookUrls generateDifferentThan projectHookId.projectHookUrl)))
      }

      verifier.checkProjectHookPresence(projectHookId, oauthAccessToken).unsafeRunSync() shouldBe false
    }

    "fail if fetching the GitLab url fails" in new TestCase {
      val personalAccessToken = personalAccessTokens.generateOne
      val exception           = exceptions.generateOne

      expectGitLabUrlProvider(returning = IO.raiseError(exception))

      intercept[Exception] {
        verifier.checkProjectHookPresence(projectHookId, personalAccessToken).unsafeRunSync()
      } shouldBe exception
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {
      expectGitLabUrlProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        verifier.checkProjectHookPresence(projectHookId, personalAccessToken).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return a RuntimeException if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {
      expectGitLabUrlProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(serviceUnavailable().withBody("some error"))
      }

      intercept[Exception] {
        verifier.checkProjectHookPresence(projectHookId, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId/hooks returned ${Status.ServiceUnavailable}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {
      expectGitLabUrlProvider(returning = IO.pure(gitLabUrl))
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        verifier.checkProjectHookPresence(projectHookId, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId/hooks returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
    }
  }

  private trait TestCase {
    val gitLabUrl     = url(externalServiceBaseUrl)
    val selfUrl       = validatedUrls.generateOne
    val projectHookId = projectHookIds.generateOne
    val projectId     = projectHookId.projectId

    val gitLabUrlProvider = mock[IOGitLabConfigProvider]

    def expectGitLabUrlProvider(returning: IO[GitLabConfig.HostUrl]) =
      (gitLabUrlProvider.get _)
        .expects()
        .returning(returning)

    val verifier = new IOProjectHookVerifier(gitLabUrlProvider)
  }

  private def withHooks(projectId: ProjectId, oneHookUrl: ProjectHookUrl): String =
    Json
      .arr(
        hook(
          url       = projectHookUrls.generateOne,
          projectId = projectId
        ),
        hook(
          url       = oneHookUrl,
          projectId = projectId
        ),
        hook(
          url       = projectHookUrls.generateOne,
          projectId = projectId
        )
      )
      .toString()

  private def hook(projectId: ProjectId, url: ProjectHookUrl): Json = Json.obj(
    "id"         -> Json.fromInt(positiveInts().generateOne),
    "url"        -> Json.fromString(url.value),
    "project_id" -> Json.fromInt(projectId.value)
  )

  private def url(value: String) =
    RefType
      .applyRef[String Refined Url](value)
      .getOrElse(throw new IllegalArgumentException("Invalid url value"))
}
