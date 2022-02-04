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

package io.renku.commiteventservice.events.categories.common

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal._
import io.renku.commiteventservice.events.categories.common
import io.renku.commiteventservice.events.categories.common.Generators._
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.CommittedDate
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Status
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{LocalDateTime, ZoneOffset}

class CommitInfoFinderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers {

  "findCommitInfo" should {

    "fetch commit info from the configured url " +
      "and return CommitInfo if OK returned with valid body" in new TestCase {

        stubFor {
          get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
            .withAccessToken(maybeAccessToken)
            .willReturn(okJson(responseJson.toString()))
        }

        finder.findCommitInfo(projectId, commitId).unsafeRunSync() shouldBe CommitInfo(
          id = commitId,
          message = commitMessage,
          committedDate = committedDate,
          author = author,
          committer = committer,
          parents = parents
        )
      }

    "fetch commit info from the configured url " +
      "and return CommitInfo if OK returned with valid body - case with no access token" in new TestCase {

        stubFor {
          get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
            .willReturn(okJson(responseJson.toString()))
        }

        finder.findCommitInfo(projectId, commitId)(maybeAccessToken = None).unsafeRunSync() shouldBe common.CommitInfo(
          id = commitId,
          message = commitMessage,
          committedDate = committedDate,
          author = author,
          committer = committer,
          parents = parents
        )
      }

    "fallback to fetch commit info without an access token for UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .withAccessToken(maybeAccessToken)
          .willReturn(unauthorized())
      }

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .willReturn(okJson(responseJson.toString()))
      }

      finder.findCommitInfo(projectId, commitId)(maybeAccessToken = None).unsafeRunSync() shouldBe common.CommitInfo(
        id = commitId,
        message = commitMessage,
        committedDate = committedDate,
        author = author,
        committer = committer,
        parents = parents
      )
    }

    "return an Error if remote client responds with invalid json" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        finder.findCommitInfo(projectId, commitId).unsafeRunSync()
      }.getMessage should startWith(
        s"GET $gitLabUrl/api/v4/projects/$projectId/repository/commits/$commitId returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }

    "return an Error if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .willReturn(notFound().withBody("some message"))
      }

      intercept[Exception] {
        finder.findCommitInfo(projectId, commitId).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId/repository/commits/$commitId returned ${Status.NotFound}; body: some message"
    }
  }

  "getMaybeCommitInfo" should {

    "get commit info from the configured url " +
      "and return some CommitInfo if OK returned with valid body" in new TestCase {

        stubFor {
          get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
            .withAccessToken(maybeAccessToken)
            .willReturn(okJson(responseJson.toString()))
        }

        finder.getMaybeCommitInfo(projectId, commitId).unsafeRunSync() shouldBe common
          .CommitInfo(
            id = commitId,
            message = commitMessage,
            committedDate = committedDate,
            author = author,
            committer = committer,
            parents = parents
          )
          .some
      }

    "get commit info from the configured url " +
      "and return some CommitInfo if OK returned with valid body - case with no access token" in new TestCase {

        stubFor {
          get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
            .willReturn(okJson(responseJson.toString()))
        }

        finder.getMaybeCommitInfo(projectId, commitId)(maybeAccessToken = None).unsafeRunSync() shouldBe common
          .CommitInfo(
            id = commitId,
            message = commitMessage,
            committedDate = committedDate,
            author = author,
            committer = committer,
            parents = parents
          )
          .some
      }

    "return None if remote client responds with Not found" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .willReturn(notFound())
      }

      finder.getMaybeCommitInfo(projectId, commitId).unsafeRunSync() shouldBe None

    }

    "fallback to fetch commit info without an access token for UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .withAccessToken(maybeAccessToken)
          .willReturn(unauthorized())
      }
      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .willReturn(okJson(responseJson.toString()))
      }

      finder.getMaybeCommitInfo(projectId, commitId).unsafeRunSync() shouldBe common
        .CommitInfo(
          id = commitId,
          message = commitMessage,
          committedDate = committedDate,
          author = author,
          committer = committer,
          parents = parents
        )
        .some
    }

    "return an Error if remote client responds with invalid json" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        finder.findCommitInfo(projectId, commitId).unsafeRunSync()
      }.getMessage should startWith(
        s"GET $gitLabUrl/api/v4/projects/$projectId/repository/commits/$commitId returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }

    "return an Error if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits/$commitId")
          .willReturn(notFound().withBody("some message"))
      }

      intercept[Exception] {
        finder.findCommitInfo(projectId, commitId).unsafeRunSync()
      }.getMessage shouldBe s"GET $gitLabUrl/api/v4/projects/$projectId/repository/commits/$commitId returned ${Status.NotFound}; body: some message"
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateSome

    val gitLabUrl     = GitLabUrl(externalServiceBaseUrl)
    val projectId     = projectIds.generateOne
    val commitId      = commitIds.generateOne
    val commitMessage = commitMessages.generateOne
    val committedDate = CommittedDate(LocalDateTime.of(2012, 9, 20, 9, 6, 12).atOffset(ZoneOffset.ofHours(3)).toInstant)
    val author        = authors.generateOne
    val committer     = committers.generateOne
    val parents       = parentsIdsLists().generateOne
    private implicit val logger: TestLogger[IO] = TestLogger()
    val finder = new CommitInfoFinderImpl[IO](gitLabUrl, Throttler.noThrottling)

    lazy val responseJson = json"""{
      "id":              ${commitId.value},
      "author_name":     ${author.name.value},
      "author_email":    ${author.emailToJson},
      "committer_name":  ${committer.name.value},
      "committer_email": ${committer.emailToJson},
      "message":         ${commitMessage.value},
      "committed_date":  "2012-09-20T09:06:12+03:00",
      "parent_ids":      ${parents.map(_.value)}
    }"""
  }
}
