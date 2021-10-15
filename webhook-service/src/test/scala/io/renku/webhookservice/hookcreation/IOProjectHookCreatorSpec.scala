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

package io.renku.webhookservice.hookcreation

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GitLabUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Json
import io.renku.webhookservice.WebhookServiceGenerators.{projectHookUrls, serializedHookTokens}
import io.renku.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import org.http4s.Status
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class IOProjectHookCreatorSpec extends AnyWordSpec with MockFactory with ExternalServiceStubbing with should.Matchers {

  "create" should {

    "send relevant Json payload and 'PRIVATE-TOKEN' header (when Personal Access Token is given) " +
      "and return Unit if the remote responds with CREATED" in new TestCase {

        val personalAccessToken = personalAccessTokens.generateOne

        stubFor {
          post(s"/api/v4/projects/$projectId/hooks")
            .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
            .withRequestBody(equalToJson(toJson(projectHook)))
            .willReturn(created())
        }

        hookCreator.create(projectHook, personalAccessToken).unsafeRunSync() shouldBe ((): Unit)
      }

    "send relevant Json payload and 'Authorization' header (when OAuth Access Token is given) " +
      "and return Unit if the remote responds with CREATED" in new TestCase {

        val oauthAccessToken = oauthAccessTokens.generateOne

        stubFor {
          post(s"/api/v4/projects/$projectId/hooks")
            .withHeader("Authorization", equalTo(s"Bearer ${oauthAccessToken.value}"))
            .withRequestBody(equalToJson(toJson(projectHook)))
            .willReturn(created())
        }

        hookCreator.create(projectHook, oauthAccessToken).unsafeRunSync() shouldBe ((): Unit)
      }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {

      val accessToken = accessTokens.generateOne

      stubFor {
        post(s"/api/v4/projects/$projectId/hooks")
          .withRequestBody(equalToJson(toJson(projectHook)))
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        hookCreator.create(projectHook, accessToken).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return an Exception if remote client responds with status neither CREATED nor UNAUTHORIZED" in new TestCase {

      val accessToken = accessTokens.generateOne

      stubFor {
        post(s"/api/v4/projects/$projectId/hooks")
          .withRequestBody(equalToJson(toJson(projectHook)))
          .willReturn(badRequest().withBody("some message"))
      }

      intercept[Exception] {
        hookCreator.create(projectHook, accessToken).unsafeRunSync()
      }.getMessage shouldBe s"POST $gitLabUrl/api/v4/projects/$projectId/hooks returned ${Status.BadRequest}; body: some message"
    }
  }

  private implicit val cs:    ContextShift[IO] = IO.contextShift(global)
  private implicit val timer: Timer[IO]        = IO.timer(global)

  private trait TestCase {
    val projectHook = projectHooks.generateOne
    val projectId   = projectHook.projectId
    val gitLabUrl   = GitLabUrl(externalServiceBaseUrl)

    def toJson(projectHook: ProjectHook) =
      Json
        .obj(
          "id"          -> Json.fromInt(projectHook.projectId.value),
          "url"         -> Json.fromString(projectHook.projectHookUrl.value),
          "push_events" -> Json.fromBoolean(true),
          "token"       -> Json.fromString(projectHook.serializedHookToken.value)
        )
        .toString()

    val hookCreator = new IOProjectHookCreator(gitLabUrl, Throttler.noThrottling, TestLogger())
  }

  private implicit lazy val projectHooks: Gen[ProjectHook] = for {
    projectId           <- projectIds
    hookUrl             <- projectHookUrls
    serializedHookToken <- serializedHookTokens
  } yield ProjectHook(projectId, hookUrl, serializedHookToken)
}
