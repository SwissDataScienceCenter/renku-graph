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

package io.renku.webhookservice.hookvalidation

import cats.effect.{ContextShift, IO, Timer}
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Json
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.projects.Id
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.webhookservice.WebhookServiceGenerators._
import io.renku.webhookservice.hookvalidation.ProjectHookVerifier.HookIdentifier
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.Status
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOProjectHookVerifierSpec extends AnyWordSpec with MockFactory with ExternalServiceStubbing with should.Matchers {

  "checkHookPresence" should {

    "return true if there's a hook with url pointing to expected project hook url - personal access token case" in new TestCase {

      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson(withHooks(projectId, oneHookUrl = projectHookId.projectHookUrl)))
      }

      verifier.checkHookPresence(projectHookId, personalAccessToken).unsafeRunSync() shouldBe true
    }

    "return true if there's a hook with url pointing to expected project hook url - oauth token case" in new TestCase {

      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("Authorization", equalTo(s"Bearer ${oauthAccessToken.value}"))
          .willReturn(okJson(withHooks(projectId, oneHookUrl = projectHookId.projectHookUrl)))
      }

      verifier.checkHookPresence(projectHookId, oauthAccessToken).unsafeRunSync() shouldBe true
    }

    "return false if there's no hook with url pointing to expected project hook url" in new TestCase {

      val oauthAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("Authorization", equalTo(s"Bearer ${oauthAccessToken.value}"))
          .willReturn(
            okJson(
              withHooks(projectId, oneHookUrl = projectHookUrls generateDifferentThan projectHookId.projectHookUrl)
            )
          )
      }

      verifier.checkHookPresence(projectHookId, oauthAccessToken).unsafeRunSync() shouldBe false
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {

      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        verifier.checkHookPresence(projectHookId, personalAccessToken).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return a RuntimeException if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(serviceUnavailable().withBody("some error"))
      }

      intercept[Exception] {
        verifier.checkHookPresence(projectHookId, personalAccessToken).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId/hooks returned ${Status.ServiceUnavailable}; body: some error"
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {

      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/hooks")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        verifier.checkHookPresence(projectHookId, personalAccessToken).unsafeRunSync()
      }.getMessage should startWith(
        s"GET $gitLabUrl/api/v4/projects/$projectId/hooks returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val gitLabUrl     = GitLabUrl(externalServiceBaseUrl)
    val projectHookId = projectHookIds.generateOne
    val projectId     = projectHookId.projectId

    val verifier = new IOProjectHookVerifier(gitLabUrl, Throttler.noThrottling, TestLogger())
  }

  private def withHooks(projectId: Id, oneHookUrl: ProjectHookUrl): String =
    Json
      .arr(
        hook(
          url = projectHookUrls.generateOne,
          projectId = projectId
        ),
        hook(
          url = oneHookUrl,
          projectId = projectId
        ),
        hook(
          url = projectHookUrls.generateOne,
          projectId = projectId
        )
      )
      .toString()

  private def hook(projectId: Id, url: ProjectHookUrl): Json = Json.obj(
    "id"         -> Json.fromInt(positiveInts().generateOne.value),
    "url"        -> Json.fromString(url.value),
    "project_id" -> Json.fromInt(projectId.value)
  )

  private lazy val projectHookIds: Gen[HookIdentifier] = for {
    projectId <- projectIds
    hookUrl   <- projectHookUrls
  } yield HookIdentifier(projectId, hookUrl)
}
