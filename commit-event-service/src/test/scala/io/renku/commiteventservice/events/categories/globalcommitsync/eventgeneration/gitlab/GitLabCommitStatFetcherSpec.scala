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

package io.renku.commiteventservice.events.categories.globalcommitsync
package eventgeneration.gitlab

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Encoder
import io.circe.literal.JsonStringContext
import io.circe.syntax._
import io.renku.commiteventservice.events.categories.globalcommitsync.Generators.commitsCounts
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats
import io.renku.generators.CommonGraphGenerators.personalAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.projects
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Request, Response, Status, Uri}
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
    with ScalaCheckPropertyChecks
    with GitLabClientTools[IO] {

  "fetchCommitStats" should {

    "return a ProjectCommitStats with count of commits " in new TestCase {
      val maybeLatestCommit = commitIds.generateOption
      givenLatestCommitFetcher(returning = maybeLatestCommit.pure[IO])

      val commitCount = commitsCounts.generateOne
      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[CommitsCount]])(
          _: Option[AccessToken]
        ))
        .expects(uri, endpointName, *, maybeAccessToken)
        .returning(commitCount.some.pure[IO])

      gitLabCommitStatFetcher.fetchCommitStats(projectId).unsafeRunSync() shouldBe ProjectCommitStats(
        maybeLatestCommit,
        commitCount
      ).some
    }

    "return None if the gitlab API returns no statistics" in new TestCase {
      val maybeLatestCommit = commitIds.generateOption
      givenLatestCommitFetcher(returning = maybeLatestCommit.pure[IO])

      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[CommitsCount]])(
          _: Option[AccessToken]
        ))
        .expects(uri"projects" / projectId.show withQueryParam ("statistics", "true"), endpointName, *, *)
        .returning(None.pure[IO])

      gitLabCommitStatFetcher.fetchCommitStats(projectId).unsafeRunSync() shouldBe None
    }

    "extract commitCount if GitLab responds with OK" in new TestCase {
      givenLatestCommitFetcher(returning = commitIds.generateOption.pure[IO])

      val commitsCount = commitsCounts.generateOne
      mapResponse(Status.Ok, Request[IO](), Response[IO](Status.Ok).withEntity(commitsCount.asJson))
        .unsafeRunSync() shouldBe commitsCount.some
    }

    Status.NotFound :: Status.InternalServerError :: Status.Unauthorized :: Status.Forbidden :: Nil foreach { status =>
      s"return None if Gitlab responds with $status" in new TestCase {
        givenLatestCommitFetcher(returning = commitIds.generateOption.pure[IO])

        mapResponse(status, Request[IO](), Response[IO](status)).unsafeRunSync() shouldBe None
      }
    }
  }

  private trait TestCase {

    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateSome
    val projectId = projectIds.generateOne

    private implicit val logger: TestLogger[IO]   = TestLogger[IO]()
    implicit val gitLabClient:   GitLabClient[IO] = mock[GitLabClient[IO]]
    val gitLabCommitFetcher     = mock[GitLabCommitFetcher[IO]]
    val gitLabCommitStatFetcher = new GitLabCommitStatFetcherImpl[IO](gitLabCommitFetcher)

    val uri = uri"projects" / projectId.show withQueryParams Map("statistics" -> true)
    val endpointName: String Refined NonEmpty = "single-project"

    lazy val mapResponse = captureMapping(gitLabCommitStatFetcher, gitLabClient)(
      _.fetchCommitStats(projectId).unsafeRunSync(),
      commitsCounts.generateOption
    )

    def givenLatestCommitFetcher(returning: IO[Option[CommitId]]) =
      (gitLabCommitFetcher
        .fetchLatestGitLabCommit(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(returning)
  }

  private implicit lazy val commitsCountEncoder: Encoder[CommitsCount] = Encoder.instance { count =>
    json"""{
      "statistics": {
        "commit_count": ${count.value}
      }
    }"""
  }
}
