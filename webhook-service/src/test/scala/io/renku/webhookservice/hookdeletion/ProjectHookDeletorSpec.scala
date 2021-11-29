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

package io.renku.webhookservice.hookdeletion

import cats.effect.IO
import com.github.tomakehurst.wiremock.client.WireMock._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.{accessTokens, personalAccessTokens}
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.webhookservice.WebhookServiceGenerators.{hookIdAndUrls, projectHookUrls, serializedHookTokens}
import io.renku.webhookservice.hookdeletion.HookDeletor.DeletionResult
import io.renku.webhookservice.hookdeletion.ProjectHookDeletor.ProjectHook
import org.http4s.Status
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectHookDeletorSpec
    extends AnyWordSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers
    with IOSpec {

  "delete" should {

    "send relevant header (when Personal Access Token is given) " +
      "and return Ok if the remote responds with OK" in new TestCase {

        val personalAccessToken = personalAccessTokens.generateOne

        stubFor {
          delete(s"/api/v4/projects/$projectId/hooks/${hookIdAndUrl.id}")
            .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
            .willReturn(ok())
        }

        hookDeletor
          .delete(projectId, hookIdAndUrl, personalAccessToken)
          .unsafeRunSync() shouldBe DeletionResult.HookDeleted
      }

    "send relevant header (when Personal Access Token is given) " +
      "and return NotFound if the remote responds with NOT_FOUND" in new TestCase {

        val personalAccessToken = personalAccessTokens.generateOne

        stubFor {
          delete(s"/api/v4/projects/$projectId/hooks/${hookIdAndUrl.id}")
            .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
            .willReturn(notFound())
        }

        hookDeletor
          .delete(projectId, hookIdAndUrl, personalAccessToken)
          .unsafeRunSync() shouldBe DeletionResult.HookNotFound
      }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {

      val accessToken = accessTokens.generateOne

      stubFor {
        delete(s"/api/v4/projects/$projectId/hooks/${hookIdAndUrl.id}")
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        hookDeletor.delete(projectId, hookIdAndUrl, accessToken).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return an Exception if remote client responds with status neither OK, NOT_FOUND or UNAUTHORIZED" in new TestCase {

      val accessToken = accessTokens.generateOne

      stubFor {
        delete(s"/api/v4/projects/$projectId/hooks/${hookIdAndUrl.id}")
          .willReturn(badRequest().withBody("some message"))
      }

      intercept[Exception] {
        hookDeletor.delete(projectId, hookIdAndUrl, accessToken).unsafeRunSync()
      }.getMessage shouldBe s"DELETE $gitLabUrl/api/v4/projects/$projectId/hooks/${hookIdAndUrl.id} returned ${Status.BadRequest}; body: some message"
    }
  }

  private trait TestCase {
    val hookIdAndUrl = hookIdAndUrls.generateOne
    val projectId    = projectIds.generateOne
    val gitLabUrl    = GitLabUrl(externalServiceBaseUrl)

    implicit val logger: TestLogger[IO] = TestLogger[IO]()

    val hookDeletor = new ProjectHookDeletorImpl[IO](gitLabUrl, Throttler.noThrottling)
  }

  private implicit lazy val projectHooks: Gen[ProjectHook] = for {
    projectId           <- projectIds
    hookUrl             <- projectHookUrls
    serializedHookToken <- serializedHookTokens
  } yield ProjectHook(projectId, hookUrl, serializedHookToken)
}
