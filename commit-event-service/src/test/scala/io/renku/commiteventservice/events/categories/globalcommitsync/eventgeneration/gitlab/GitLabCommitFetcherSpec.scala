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
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json
import io.circe.literal._
import io.renku.commiteventservice.events.categories.common.CommitInfo
import io.renku.commiteventservice.events.categories.common.Generators.commitInfos
import io.renku.commiteventservice.events.categories.globalcommitsync.eventgeneration.PageResult
import io.renku.generators.CommonGraphGenerators.{oauthAccessTokens, pages, pagingRequests, personalAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectIds
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.RestClientError.{BadRequestException, UnauthorizedException}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.http4s.Method.GET
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Header, Method, Request, Response, Status, Uri}
import org.scalamock.matchers.ArgCapture.CaptureOne
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.ci.CIStringSyntax

class GitLabCommitFetcherSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "fetchGitLabCommits" should {

    "fetch commits from the given page" in new TestCase {

      val expectation = pageResults.generateOne

      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, PageResult])(
          _: Option[AccessToken]
        ))
        .expects(GET, uri, endpointName, *, maybeAccessToken)
        .returning(expectation.pure[IO])

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId, pageRequest)(None)
        .unsafeRunSync() shouldBe expectation
    }


    "fetch commits using the given personal access token" in new TestCase {
      val personalAccessToken = personalAccessTokens.generateOne

      val expectation = pageResults.generateOne

      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, PageResult])(
          _: Option[AccessToken]
        ))
        .expects(GET, uri, endpointName, *, Some(personalAccessToken))
        .returning(expectation.pure[IO])

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId, pageRequest)(Some(personalAccessToken))
        .unsafeRunSync() shouldBe expectation
    }

    "fetch commits using the given oauth token" in new TestCase {
      val authAccessToken = oauthAccessTokens.generateOne

      val expectation = pageResults.generateOne

      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, PageResult])(
          _: Option[AccessToken]
        ))
        .expects(GET, uri, endpointName, *, authAccessToken.some)
        .returning(expectation.pure[IO])

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId, pageRequest)(Some(authAccessToken))
        .unsafeRunSync() shouldBe expectation
    }

    "return no commits if there aren't any" in new TestCase {

      val expectation = PageResult(commits = Nil, maybeNextPage = None)

      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, PageResult])(
          _: Option[AccessToken]
        ))
        .expects(GET, uri, endpointName, *, maybeAccessToken)
        .returning(expectation.pure[IO])

      gitLabCommitFetcher
        .fetchGitLabCommits(projectId, pageRequest)(maybeAccessToken = None)
        .unsafeRunSync() shouldBe expectation
    }


    /////////////////// ResponseMapper tests


    "fetch commits from the given page - responseMapping OK" in new TestCase {

      val maybeNextPage = pages.generateOption

      multiCommitResponseMapping
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

    "fetch commits from the given page - responseMapping NotFound" in new TestCase {

      multiCommitResponseMapping
        .value(
          (Status.NotFound, Request[IO](), Response[IO]())
        )
        .unsafeRunSync() shouldBe PageResult.empty.pure[IO]
    }

    "fetch commits from the given page - responseMapping Unauthorized" in new TestCase {

      multiCommitResponseMapping
        .value(
          (Status.Unauthorized, Request[IO](), Response[IO]())
        )
        .unsafeRunSync() shouldBe UnauthorizedException.raiseError[IO, UnauthorizedException]
    }


    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      multiCommitResponseMapping
        .value(
          (Status.BadRequest, Request[IO](), Response[IO]())
        )
        .unsafeRunSync() shouldBe BadRequestException.raiseError

    }

    "return an Exception if remote client responds with unexpected body" in new TestCase {

      intercept[Exception] {
        multiCommitResponseMapping
          .value(
            (Status.Unauthorized, Request[IO](), Response[IO]())
          )
          .unsafeRunSync()
      }.getMessage should startWith(
        s"/projects/$projectId/repository/commits?page=${pageRequest.page}&per_page=${pageRequest.perPage} returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }
  }

  ///////////// Single commit tesst


  "fetchLatestGitLabCommit" should {

    "return a single commit" in new TestCase {
      stubFor {
        get(s"/api/v4/projects/$projectId/repository/commits?per_page=1")
          .willReturn(okJson(commitsJson(from = List(commitInfoList.head))))
      }
      gitLabCommitFetcher.fetchLatestGitLabCommit(projectId)(None).unsafeRunSync() shouldBe Some(commitInfoList.head.id)
    }

    /////////////////// Single commit response mapper


    "fetch the latest commit from the given page - responseMapping OK" in new TestCase {

      val maybeNextPage = pages.generateOption

      singleCommitResponseMapping
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

    "fetch the latest commit from the given page - responseMapping NotFound" in new TestCase {

      singleCommitResponseMapping
        .value(
          (Status.NotFound, Request[IO](), Response[IO]())
        )
        .unsafeRunSync() shouldBe PageResult.empty.pure[IO]
    }

    "fetch the latest commit from the given page - responseMapping Unauthorized" in new TestCase {

      singleCommitResponseMapping
        .value(
          (Status.Unauthorized, Request[IO](), Response[IO]())
        )
        .unsafeRunSync() shouldBe UnauthorizedException.raiseError[IO, UnauthorizedException]
    }


    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      singleCommitResponseMapping
        .value(
          (Status.BadRequest, Request[IO](), Response[IO]())
        )
        .unsafeRunSync() shouldBe BadRequestException.raiseError

    }

    "return an Exception if remote client responds with unexpected body" in new TestCase {

      intercept[Exception] {
        singleCommitResponseMapping
          .value(
            (Status.Unauthorized, Request[IO](), Response[IO]())
          )
          .unsafeRunSync()
      }.getMessage should startWith(
        s"/projects/$projectId/repository/commits?page=${pageRequest.page}&per_page=${pageRequest.perPage} returned ${Status.Ok}; error: Invalid message body: Could not decode JSON: {}"
      )
    }

  }








  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = personalAccessTokens.generateSome
    val projectId      = projectIds.generateOne
    val pageRequest    = pagingRequests.generateOne
    val commitInfoList = commitInfos.generateNonEmptyList().toList

    val gitLabClient = mock[GitLabClient[IO]]
    private implicit val logger: TestLogger[IO] = TestLogger()
    val gitLabCommitFetcher = new GitLabCommitFetcherImpl[IO](gitLabClient)

    val uri = uri"/projects" / projectId.show / "repository" / "commits" withQueryParams Map(
      "page"     -> pageRequest.page.show,
      "per_page" -> pageRequest.perPage.show
    )

    val endpointName: String Refined NonEmpty = "commits"

    lazy val multiCommitResponseMapping = responseMapping(true)

    lazy val singleCommitResponseMapping = responseMapping(false)

    def responseMapping(isMulti: Boolean) = {
      val responseMapping = CaptureOne[ResponseMappingF[IO, PageResult]]()

      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, PageResult])(
          _: Option[AccessToken]
        ))
        .expects(*, *, *, capture(responseMapping), *)
        .returning(pageResults.generateOne.pure[IO])

      if (isMulti) {
        gitLabCommitFetcher
          .fetchGitLabCommits(projectId, pageRequest)(None)
          .unsafeRunSync()
      } else {
        gitLabCommitFetcher
          .fetchLatestGitLabCommit(projectId)(None)
          .unsafeRunSync()
      }


      responseMapping
    }

  }

  private def pageResults = for {
    commits       <- commitIds.toGeneratorOfList()
    maybeNextPage <- pages.toGeneratorOfOptions
  } yield PageResult(commits, maybeNextPage)

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
