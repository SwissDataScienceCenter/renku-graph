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
import io.renku.commiteventservice.events.categories.globalcommitsync.Generators.commitsCounts
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.{PageResult, ProjectCommitStats}
import io.renku.generators.CommonGraphGenerators.personalAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.graph.model.projects
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import org.http4s.Method.GET
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Method, Request, Response, Status, Uri}
import org.scalamock.matchers.ArgCapture.CaptureOne
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

        (gitLabClient
          .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[CommitsCount]])(
            _: Option[AccessToken]
          ))
          .expects(GET, uri, endpointName, *, maybeAccessToken)
          .returning(commitCount.some.pure[IO])

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

      mapResponse(Status.NotFound, Request[IO](), Response[IO]()).unsafeRunSync() shouldBe None

    }

    "return None if the gitlab API returns no statistics" in new TestCase {
      val maybeLatestCommit = commitIds.generateOption
      (gitLabCommitFetcher
        .fetchLatestGitLabCommit(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(maybeLatestCommit.pure[IO])

      stubFor {
        get(s"/api/v4/projects/$projectId?statistics=true")
          .willReturn(okJson("{}"))
      }

      gitLabCommitStatFetcher.fetchCommitStats(projectId).unsafeRunSync() shouldBe None
    }

    "return None if the gitlab API returns a Unauthorized" in new TestCase {
      val maybeLatestCommit = commitIds.generateOption
      (gitLabCommitFetcher
        .fetchLatestGitLabCommit(_: projects.Id)(_: Option[AccessToken]))
        .expects(projectId, maybeAccessToken)
        .returning(maybeLatestCommit.pure[IO])

      intercept[Exception] {
        mapResponse(Status.Unauthorized, Request[IO](), Response[IO]()).unsafeRunSync()
      } shouldBe a[UnauthorizedException]
    }
  }

  private trait TestCase {

    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateSome
    val projectId = projectIds.generateOne

    val gitLabCommitFetcher = mock[GitLabCommitFetcher[IO]]
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabClient = mock[GitLabClient[IO]]
    val gitLabCommitStatFetcher =
      new GitLabCommitStatFetcherImpl[IO](gitLabCommitFetcher, gitLabClient)

    val uri = uri"projects" / projectId.show withQueryParams Map("statistics" -> true)
    val endpointName: String Refined NonEmpty = "projects"

    lazy val mapResponse = {
      val responseMapping = CaptureOne[ResponseMappingF[IO, PageResult]]()

      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[CommitsCount]])(
          _: Option[AccessToken]
        ))
        .expects(*, *, *, capture(responseMapping), *)
        .returning(None.pure[IO])

      gitLabCommitStatFetcher.fetchCommitStats(projectId).unsafeRunSync()

      responseMapping.value
    }
  }

  private implicit lazy val commitsCountEncoder: Encoder[CommitsCount] = Encoder.instance { count =>
    json"""{
      "statistics": {
        "commit_count": ${count.value}
      }
    }"""
  }
}
