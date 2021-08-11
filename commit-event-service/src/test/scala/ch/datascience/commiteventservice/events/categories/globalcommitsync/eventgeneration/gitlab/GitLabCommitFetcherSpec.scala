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

package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.gitlab

import cats.effect.{ConcurrentEffect, IO, Timer}
import ch.datascience.commiteventservice.events.categories.common.CommitInfo
import ch.datascience.commiteventservice.events.categories.common.Generators.commitInfos
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators.{oauthAccessTokens, personalAccessTokens}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GitLabUrl
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.model.events.CommitId
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Json
import io.circe.literal._
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class GitLabCommitFetcherSpec extends AnyWordSpec with MockFactory with ExternalServiceStubbing with should.Matchers {

  "fetchGitLabCommits" should {

    "handle a paged request" in new TestCase {
      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits")
          .willReturn(okJson(commitsJson(from = List(commitInfoList.head))).withHeader("X-Next-Page", "2"))
      }

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?page=2")
          .willReturn(okJson(commitsJson(from = commitInfoList.tail)).withHeader("X-Next-Page", ""))
      }

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId)(None)
        .unsafeRunSync() shouldBe commitInfoList.map(
        _.id
      )
    }

    "fetch commits from GitLab - personal access token case " in new TestCase {
      val personalAccessToken = personalAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits")
          .withHeader("PRIVATE-TOKEN", equalTo(personalAccessToken.value))
          .willReturn(okJson(commitsJson(from = commitInfoList)))
      }

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId)(Some(personalAccessToken))
        .unsafeRunSync() shouldBe commitInfoList.map(
        _.id
      )
    }

    "fetch commits from GitLab - oauth token case " in new TestCase {
      val authAccessToken = oauthAccessTokens.generateOne

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits")
          .withHeader("Authorization", equalTo(s"Bearer ${authAccessToken.value}"))
          .willReturn(okJson(commitsJson(from = commitInfoList)))
      }

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId)(Some(authAccessToken))
        .unsafeRunSync() shouldBe commitInfoList.map(
        _.id
      )
    }

    "fetch commits from GitLab - no token case " in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits")
          .willReturn(okJson(commitsJson(from = commitInfoList)))
      }

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId)(maybeAccessToken = None)
        .unsafeRunSync() shouldBe commitInfoList.map(
        _.id
      )
    }

    "fetch no commits from GitLab if there aren't any" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits")
          .willReturn(okJson("[]"))
      }

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId)(maybeAccessToken = None)
        .unsafeRunSync() shouldBe List.empty[CommitId]
    }

    "return an empty list if project not found" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits")
          .willReturn(notFound())
      }

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId)(maybeAccessToken = None)
        .unsafeRunSync() shouldBe List.empty[CommitId]
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits")
          .willReturn(unauthorized())
      }

      intercept[Exception] {
        gitLabCommitFetcher
          .fetchGitLabCommits(projectId)(maybeAccessToken = None)
          .unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits")
          .willReturn(badRequest().withBody("some error"))
      }

      intercept[Exception] {
        gitLabCommitFetcher
          .fetchGitLabCommits(projectId)(maybeAccessToken = None)
          .unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabApiUrl/projects/$projectId/repository/commits returned ${Status.BadRequest}; body: some error"
    }

    "return an Exception if remote client responds with unexpected body" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        gitLabCommitFetcher
          .fetchGitLabCommits(projectId)(maybeAccessToken = None)
          .unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabApiUrl/projects/$projectId/repository/commits returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
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

  private implicit val ce:    ConcurrentEffect[IO] = IO.ioConcurrentEffect(IO.contextShift(global))
  private implicit val timer: Timer[IO]            = IO.timer(global)

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateSome
    val projectId      = projectIds.generateOne
    val commitInfoList = commitInfos.generateNonEmptyList().toList

    val gitLabApiUrl        = GitLabUrl(externalServiceBaseUrl).apiV4
    val gitLabCommitFetcher = new GitLabCommitFetcherImpl[IO](gitLabApiUrl, Throttler.noThrottling, TestLogger())
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
