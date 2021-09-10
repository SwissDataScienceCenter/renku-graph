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
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.globalcommitsync.Generators.commitCounts
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats
import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats.CommitCount
import ch.datascience.control.Throttler
import ch.datascience.generators.CommonGraphGenerators.personalAccessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators.projectIds
import ch.datascience.graph.model.{GitLabUrl, projects}
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.interpreters.TestLogger
import ch.datascience.stubbing.ExternalServiceStubbing
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.literal.JsonStringContext
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.ExecutionContext.Implicits.global

class GitLabCommitStatFetcherSpec
    extends AnyWordSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers
    with ScalaCheckPropertyChecks {
  "fetchCommitStats" should {
    "return a ProjectCommitStats with count of commits " in new TestCase {
      forAll(commitIds.toGeneratorOfOptions, commitCounts) { (maybeLatestCommit, commitCount) =>
        (gitLabCommitFetcher
          .fetchLatestGitLabCommit(_: projects.Id)(_: Option[AccessToken]))
          .expects(projectId, maybeAccessToken)
          .returning(maybeLatestCommit.pure[IO])

        stubFor {
          get(s"/api/v4/projects/$projectId?statistics=true")
            .willReturn(okJson(jsonCommitCount(commitCount)))
        }

        gitLabCommitStatFetcher.fetchCommitStats(projectId).unsafeRunSync() shouldBe ProjectCommitStats(
          maybeLatestCommit,
          commitCount
        )
      }

    }

    "throw an UnauthorizedException if the gitlab API returns an UnauthorizedException" in new TestCase {
      val maybeLatestCommit = commitIds.generateOption
      (gitLabCommitFetcher
        .fetchLatestGitLabCommit(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(maybeLatestCommit.pure[IO])

      stubFor(get(s"/api/v4/projects/$projectId?statistics=true").willReturn(unauthorized()))

      intercept[Exception] {
        gitLabCommitStatFetcher.fetchCommitStats(projectId).unsafeRunSync()
      } shouldBe a[UnauthorizedException]
    }
  }

  private implicit val ce:    ConcurrentEffect[IO] = IO.ioConcurrentEffect(IO.contextShift(global))
  private implicit val timer: Timer[IO]            = IO.timer(global)

  private trait TestCase {
    val projectId: projects.Id = projectIds.generateOne

    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateSome

    val gitLabUrl           = GitLabUrl(externalServiceBaseUrl)
    val gitLabCommitFetcher = mock[GitLabCommitFetcher[IO]]

    val gitLabCommitStatFetcher =
      new GitLabCommitStatFetcherImpl[IO](gitLabCommitFetcher, gitLabUrl.apiV4, Throttler.noThrottling, TestLogger())
  }

  private def jsonCommitCount(count: CommitCount) =
    json"""{
           "statistics": {
             "commit_count": ${count.value}
           }
          }""".noSpaces
}
