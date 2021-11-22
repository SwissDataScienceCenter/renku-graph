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

package io.renku.commiteventservice.events.categories.globalcommitsync
package eventgeneration.gitlab

import Generators.commitsCounts
import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import io.circe.Encoder
import io.circe.literal.JsonStringContext
import io.circe.syntax._
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats
import io.renku.control.Throttler
import io.renku.generators.CommonGraphGenerators.personalAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.http.client.AccessToken
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class GitLabCommitStatFetcherSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers
    with ScalaCheckPropertyChecks {

  "fetchCommitStats" should {
    "return a ProjectCommitStats with count of commits " in new TestCase {
      forAll(commitIds.toGeneratorOfOptions, commitsCounts) { (maybeLatestCommit, commitCount) =>
        (gitLabCommitFetcher
          .fetchLatestGitLabCommit(_: projects.Id)(_: Option[AccessToken]))
          .expects(projectId, maybeAccessToken)
          .returning(maybeLatestCommit.pure[IO])

        stubFor {
          get(s"/api/v4/projects/$projectId?statistics=true")
            .willReturn(okJson(commitCount.asJson.noSpaces))
        }

        gitLabCommitStatFetcher.fetchCommitStats(projectId).unsafeRunSync() shouldBe ProjectCommitStats(
          maybeLatestCommit,
          commitCount
        ).some
      }
    }

    "return None if the gitlab API returns a NotFound" in new TestCase {
      val maybeLatestCommit = commitIds.generateOption
      (gitLabCommitFetcher
        .fetchLatestGitLabCommit(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(maybeLatestCommit.pure[IO])

      stubFor {
        get(s"/api/v4/projects/$projectId?statistics=true")
          .willReturn(notFound())
      }

      gitLabCommitStatFetcher.fetchCommitStats(projectId).unsafeRunSync() shouldBe None
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

  private trait TestCase {

    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateSome
    val projectId = projectIds.generateOne

    val gitLabUrl           = GitLabUrl(externalServiceBaseUrl)
    val gitLabCommitFetcher = mock[GitLabCommitFetcher[IO]]
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabCommitStatFetcher =
      new GitLabCommitStatFetcherImpl[IO](gitLabCommitFetcher, gitLabUrl.apiV4, Throttler.noThrottling)
  }

  private implicit lazy val commitsCountEncoder: Encoder[CommitsCount] = Encoder.instance { count =>
    json"""{
      "statistics": {
        "commit_count": ${count.value}
      }
    }"""
  }
}
