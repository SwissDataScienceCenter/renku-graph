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

package io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.gitlab

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json
import io.circe.literal._
import io.renku.commiteventservice.events.categories.common.CommitInfo
import io.renku.commiteventservice.events.categories.common.Generators.commitInfos
import io.renku.commiteventservice.events.categories.globalcommitsync.Generators._
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.{DateCondition, PageResult}
import io.renku.generators.CommonGraphGenerators.{accessTokens, pages, pagingRequests}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.http4s.Method.GET
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{EmptyBody, Header, Method, Request, Response, Status, Uri}
import org.scalamock.matchers.ArgCapture.CaptureOne
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.ci._

class GitLabCommitFetcherSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "fetchGitLabCommits" should {

    "fetch commits from the given page" in new AllCommitsEndpointTestCase {

      val expectation = pageResults().generateOne
      val condition   = dateConditions.generateOne

      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, PageResult])(
          _: Option[AccessToken]
        ))
        .expects(GET, uri(condition), endpointName, *, maybeAccessToken)
        .returning(expectation.pure[IO])

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId, condition, pageRequest)(maybeAccessToken)
        .unsafeRunSync() shouldBe expectation
    }

    "return no commits if there aren't any" in new AllCommitsEndpointTestCase {

      val condition  = dateConditions.generateOne
      val pageResult = PageResult(commits = Nil, maybeNextPage = None)

      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, PageResult])(
          _: Option[AccessToken]
        ))
        .expects(GET, uri(condition), endpointName, *, maybeAccessToken)
        .returning(pageResult.pure[IO])

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId, condition, pageRequest)(maybeAccessToken)
        .unsafeRunSync() shouldBe pageResult
    }

    "fetch commits from the given page - response OK" in new AllCommitsEndpointTestCase {

      val maybeNextPage = pages.generateOption

      responseMapping(dateConditions.generateOne)
        .value(
          (Status.Ok,
           Request[IO](),
           Response[IO]()
             .withEntity(commitsJson(commitInfoList))
             .withHeaders(Header.Raw(ci"X-Next-Page", maybeNextPage.map(_.show).getOrElse("")))
          )
        )
        .unsafeRunSync() shouldBe PageResult(commitInfoList.map(_.id), maybeNextPage)
    }

    "fetch commits from the given page - response NotFound" in new AllCommitsEndpointTestCase {

      responseMapping(dateConditions.generateOne)
        .value(
          (Status.NotFound, Request[IO](), Response[IO]())
        )
        .unsafeRunSync() shouldBe PageResult.empty
    }

    Status.Unauthorized :: Status.Forbidden :: Nil foreach { status =>
      s"fallback to fetch commits without an access token for $status" in new AllCommitsEndpointTestCase {

        val condition  = dateConditions.generateOne
        val pageResult = PageResult(commits = commitIds.generateFixedSizeList(1), maybeNextPage = None)

        (gitLabClient
          .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, PageResult])(
            _: Option[AccessToken]
          ))
          .expects(GET, uri(condition), endpointName, *, Option.empty[AccessToken])
          .returning(pageResult.pure[IO])

        responseMapping(condition)
          .value((status, Request[IO](), Response[IO]()))
          .unsafeRunSync() shouldBe pageResult
      }
    }

    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new AllCommitsEndpointTestCase {
      intercept[Exception] {
        responseMapping(dateConditions.generateOne)
          .value((Status.BadRequest, Request[IO](), Response[IO]()))
          .unsafeRunSync()
      }.getMessage should startWith(s"(400 Bad Request")
    }

    "return an Exception if remote client responds with unexpected body" in new AllCommitsEndpointTestCase {
      intercept[Exception] {
        responseMapping(dateConditions.generateOne)
          .value((Status.Ok, Request[IO](), Response[IO](body = EmptyBody)))
          .unsafeRunSync()
      }
    }
  }

  "fetchLatestGitLabCommit" should {

    "return a single commit" in new LatestCommitsEndpointTestCase {

      val pageResult = pageResults().generateOne

      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, PageResult])(
          _: Option[AccessToken]
        ))
        .expects(GET, uri, endpointName, *, maybeAccessToken)
        .returning(pageResult.pure[IO])

      gitLabCommitFetcher
        .fetchLatestGitLabCommit(projectId)(maybeAccessToken)
        .unsafeRunSync() shouldBe pageResult
    }

    "fetch the latest commit from the given page - response OK" in new LatestCommitsEndpointTestCase {

      val maybeNextPage = pages.generateOption

      responseMapping
        .value(
          (Status.Ok,
           Request[IO](),
           Response[IO]()
             .withEntity(commitsJson(commitInfoList))
             .withHeaders(Header.Raw(ci"X-Next-Page", maybeNextPage.map(_.show).getOrElse("")))
          )
        )
        .unsafeRunSync() shouldBe commitInfoList.head.id.some
    }

    "fetch the latest commit from the given page - response NotFound" in new LatestCommitsEndpointTestCase {
      responseMapping
        .value((Status.NotFound, Request[IO](), Response[IO]()))
        .unsafeRunSync() shouldBe None
    }

    Status.Unauthorized :: Status.Forbidden :: Nil foreach { status =>
      s"fallback to fetch latest commit without an access token for $status" in new LatestCommitsEndpointTestCase {

        val pageResult = pageResults().generateOne

        (gitLabClient
          .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, PageResult])(
            _: Option[AccessToken]
          ))
          .expects(GET, uri, endpointName, *, Option.empty[AccessToken])
          .returning(pageResult.pure[IO])

        responseMapping
          .value((status, Request[IO](), Response[IO]()))
          .unsafeRunSync() shouldBe pageResult
      }
    }

    "return an Exception if remote client responds with other status" in new LatestCommitsEndpointTestCase {
      intercept[Exception] {
        responseMapping
          .value((Status.BadRequest, Request[IO](), Response[IO]()))
          .unsafeRunSync()
      }.getMessage should startWith(s"(400 Bad Request")
    }

    "return an Exception if remote client responds with unexpected body" in new LatestCommitsEndpointTestCase {
      intercept[Exception] {
        responseMapping
          .value((Status.Ok, Request[IO](), Response[IO](body = EmptyBody)))
          .unsafeRunSync()
      }
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateSome
    val projectId      = projectIds.generateOne
    val pageRequest    = pagingRequests.generateOne
    val commitInfoList = commitInfos.generateNonEmptyList().toList

    val gitLabClient = mock[GitLabClient[IO]]
    private implicit val logger: TestLogger[IO] = TestLogger()
    val gitLabCommitFetcher = new GitLabCommitFetcherImpl[IO](gitLabClient)
  }

  private trait AllCommitsEndpointTestCase extends TestCase {

    def uri(dateCondition: DateCondition) =
      uri"projects" / projectId.show / "repository" / "commits" withQueryParams Map(
        "page"     -> pageRequest.page.show,
        "per_page" -> pageRequest.perPage.show
      ) + dateCondition.asQueryParameter

    val endpointName: String Refined NonEmpty = "commits"

    def responseMapping(condition: DateCondition) = {
      val responseMapping = CaptureOne[ResponseMappingF[IO, PageResult]]()

      val endpointName: String Refined NonEmpty = "commits"
      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, PageResult])(
          _: Option[AccessToken]
        ))
        .expects(*, *, endpointName, capture(responseMapping), maybeAccessToken)
        .returning(pageResults().generateOne.pure[IO])

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId, condition, pageRequest)(maybeAccessToken)
        .unsafeRunSync()

      responseMapping
    }
  }

  private trait LatestCommitsEndpointTestCase extends TestCase {

    val uri = uri"projects" / projectId.show / "repository" / "commits" withQueryParams Map("per_page" -> "1")

    val endpointName: String Refined NonEmpty = "latest commit"

    val responseMapping = {
      val responseMapping = CaptureOne[ResponseMappingF[IO, PageResult]]()

      val endpointName: String Refined NonEmpty = "latest commit"
      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, PageResult])(
          _: Option[AccessToken]
        ))
        .expects(*, *, endpointName, capture(responseMapping), maybeAccessToken)
        .returning(pageResults(1).generateOne.pure[IO])

      gitLabCommitFetcher
        .fetchLatestGitLabCommit(projectId)(maybeAccessToken)
        .unsafeRunSync()

      responseMapping
    }
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
