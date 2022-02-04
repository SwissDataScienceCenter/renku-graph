/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.commiteventservice.events.categories.commitsync.eventgeneration

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Json
import io.circe.literal._
import io.renku.commiteventservice.events.categories.common.CommitInfo
import io.renku.commiteventservice.events.categories.common.Generators._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators._
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class LatestCommitFinderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

  "findLatestCommit" should {

    "return latest Commit info if remote responds with OK and valid body" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?per_page=1")
          .withAccessToken(maybeAccessToken)
          .willReturn(okJson(commitsJson(from = commitInfo)))
      }

      latestCommitFinder.findLatestCommit(projectId).unsafeRunSync() shouldBe commitInfo.some
    }

    "return latest Commit info if remote responds with OK and valid body - no token case" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?per_page=1")
          .willReturn(okJson(commitsJson(from = commitInfo)))
      }

      latestCommitFinder.findLatestCommit(projectId).unsafeRunSync() shouldBe commitInfo.some
    }

    "return None if remote responds with OK and no commits" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?per_page=1")
          .willReturn(okJson("[]"))
      }

      latestCommitFinder.findLatestCommit(projectId).unsafeRunSync() shouldBe None
    }

    "return None if remote responds with NOT_FOUND" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?per_page=1")
          .willReturn(notFound())
      }

      latestCommitFinder.findLatestCommit(projectId).unsafeRunSync() shouldBe None
    }

    "fallback to fetching the latest commit without an access token for UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?per_page=1")
          .withAccessToken(maybeAccessToken)
          .willReturn(unauthorized())
      }
      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?per_page=1")
          .willReturn(okJson("[]"))
      }

      latestCommitFinder.findLatestCommit(projectId).unsafeRunSync() shouldBe None
    }

    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?per_page=1")
          .willReturn(badRequest().withBody("some error"))
      }

      intercept[Exception] {
        latestCommitFinder.findLatestCommit(projectId).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId/repository/commits?per_page=1 returned ${Status.BadRequest}; body: some error"
    }

    "return an Exception if remote client responds with unexpected body" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?per_page=1")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        latestCommitFinder.findLatestCommit(projectId).unsafeRunSync()
      }.getMessage should startWith(
        s"GET $gitLabUrl/api/v4/projects/$projectId/repository/commits?per_page=1 returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateSome

    val gitLabUrl  = GitLabUrl(externalServiceBaseUrl)
    val projectId  = projectIds.generateOne
    val commitInfo = commitInfos.generateOne
    private implicit val logger: TestLogger[IO] = TestLogger()
    val latestCommitFinder = new LatestCommitFinderImpl[IO](gitLabUrl, Throttler.noThrottling)
  }

  private def commitsJson(from: CommitInfo) =
    Json.arr(commitJson(from)).noSpaces

  private def commitJson(commitInfo: CommitInfo) = json"""{
    "id":              ${commitInfo.id.value},
    "author_name":     ${commitInfo.author.name.value},
    "author_email":    ${commitInfo.author.emailToJson},
    "committer_name":  ${commitInfo.committer.name.value},
    "committer_email": ${commitInfo.committer.emailToJson},
    "message":         ${commitInfo.message.value},
    "committed_date":  ${commitInfo.committedDate.value},
    "parent_ids":      ${commitInfo.parents.map(_.value)}
  }"""
}
