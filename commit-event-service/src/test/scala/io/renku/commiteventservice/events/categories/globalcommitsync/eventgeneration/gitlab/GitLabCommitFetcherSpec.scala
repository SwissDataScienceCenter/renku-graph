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

package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.gitlab

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Json
import io.circe.literal._
import io.renku.commiteventservice.events.categories.common.CommitInfo
import io.renku.commiteventservice.events.categories.common.Generators.commitInfos
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.PageResult
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.{oauthAccessTokens, pages, personalAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.client.AccessToken
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GitLabCommitFetcherSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

  "fetchGitLabCommits" should {

    "fetch commits from the given page" in new TestCase {

      val maybeNextPage = pages.generateOption
      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?page=$page")
          .willReturn(
            okJson(commitsJson(from = commitInfoList))
              .withHeader("X-Next-Page", maybeNextPage.map(_.show).getOrElse(""))
          )
      }

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId, page)(None)
        .unsafeRunSync() shouldBe PageResult(commitInfoList.map(_.id), maybeNextPage)
    }

    "fetch commits using the given personal access token" in new TestCase {
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?page=$page")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson(commitsJson(from = commitInfoList)))
      }

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId, page)(Some(personalAccessToken))
        .unsafeRunSync() shouldBe PageResult(commitInfoList.map(_.id), maybeNextPage = None)
    }

    "fetch commits using the given oauth token" in new TestCase {
      val authAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?page=$page")
          .withHeader("Authorization", equalTo(s"Bearer ${authAccessToken.value}"))
          .willReturn(okJson(commitsJson(from = commitInfoList)))
      }

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId, page)(Some(authAccessToken))
        .unsafeRunSync() shouldBe PageResult(commitInfoList.map(_.id), maybeNextPage = None)
    }

    "return no commits if there aren't any" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?page=$page")
          .willReturn(okJson("[]"))
      }

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId, page)(maybeAccessToken = None)
        .unsafeRunSync() shouldBe PageResult(commits = Nil, maybeNextPage = None)
    }

    "return an empty list if project for NOT_FOUND" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?page=$page")
          .willReturn(notFound())
      }

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId, page)(maybeAccessToken = None)
        .unsafeRunSync() shouldBe PageResult(commits = Nil, maybeNextPage = None)
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?page=$page")
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        gitLabCommitFetcher.fetchGitLabCommits(projectId, page)(maybeAccessToken = None).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?page=$page")
          .willReturn(badRequest().withBody("some error"))
      }

      intercept[Exception] {
        gitLabCommitFetcher
          .fetchGitLabCommits(projectId, page)(maybeAccessToken = None)
          .unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabApiUrl/projects/$projectId/repository/commits?page=$page returned ${Status.BadRequest}; body: some error"
    }

    "return an Exception if remote client responds with unexpected body" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?page=$page")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        gitLabCommitFetcher
          .fetchGitLabCommits(projectId, page)(maybeAccessToken = None)
          .unsafeRunSync()
      }.getMessage should startWith(
        s"GET $gitLabApiUrl/projects/$projectId/repository/commits?page=$page returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }
  }

  "fetchLatestGitLabCommit" should {

    "return a single commit" in new TestCase {
      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?per_page=1")
          .willReturn(okJson(commitsJson(from = List(commitInfoList.head))))
      }
      gitLabCommitFetcher.fetchLatestGitLabCommit(projectId)(None).unsafeRunSync() shouldBe Some(commitInfoList.head.id)
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateSome
    val projectId      = projectIds.generateOne
    val page           = pages.generateOne
    val commitInfoList = commitInfos.generateNonEmptyList().toList

    val gitLabApiUrl = GitLabUrl(externalServiceBaseUrl).apiV4
    private implicit val logger: TestLogger[IO] = TestLogger()
    val gitLabCommitFetcher = new GitLabCommitFetcherImpl[IO](gitLabApiUrl, Throttler.noThrottling)
  }

  private def commitsJson(from: List[CommitInfo]) =
    Json.arr(from.map(commitJson): _*).noSpaces

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
