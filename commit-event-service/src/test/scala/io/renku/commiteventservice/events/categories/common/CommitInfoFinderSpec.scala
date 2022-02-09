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
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.renku.commiteventservice.events.categories.common.Generators._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.events.CommittedDate
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.Method.GET
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Header, Method, Request, Response, Status, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.ci.CIStringSyntax

import java.time.{LocalDateTime, ZoneOffset}

class CommitInfoFinderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers
    with GitLabClientTools[IO] {

  "findCommitInfo" should {

    "fetch commit info from the configured url " +
      "and return CommitInfo if OK returned with valid body - case with Access Token" in new TestCase {
        val commitInfoExpectation = CommitInfo(
          id = commitId,
          message = commitMessage,
          committedDate = committedDate,
          author = author,
          committer = committer,
          parents = parents
        ).some

        setGitLabClientExpectation(maybeAccessToken, returning = commitInfoExpectation)

        finder.findCommitInfo(projectId, commitId)(maybeAccessToken).unsafeRunSync() shouldBe commitInfoExpectation
      }

    "fallback to fetch commit info without an access token for UNAUTHORIZED" in new TestCase {

      val result = commitInfos.generateOne
      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, CommitInfo])(
          _: Option[AccessToken]
        ))
        .expects(*, *, *, *, Option.empty[AccessToken])
        .returning(result.pure[IO])

      mapToCommitOrThrow((Status.Unauthorized, Request[IO](), Response[IO]())).unsafeRunSync() shouldBe result
    }

    "return an Error if remote client responds with invalid json" in new TestCase {

      intercept[Exception] {
        mapToCommitOrThrow(
          (Status.Ok, Request[IO](), Response[IO]().withEntity("{}").withHeaders(Header.Raw(ci"X-Next-Page", "")))
        ).unsafeRunSync()
      }
    }

    "return an Error if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      intercept[Exception] {
        mapToCommitOrThrow((Status.NotFound, Request[IO](), Response[IO]())).unsafeRunSync()
      }
    }
  }

  "getMaybeCommitInfo" should {

    "get commit info from the configured url " +
      "and return some CommitInfo if OK returned with valid body" in new TestCase {

        val someCommitInfoExpectation = CommitInfo(
          id = commitId,
          message = commitMessage,
          committedDate = committedDate,
          author = author,
          committer = committer,
          parents = parents
        ).some

        setGitLabClientExpectation(maybeAccessToken, returning = someCommitInfoExpectation)

        finder
          .getMaybeCommitInfo(projectId, commitId)(maybeAccessToken)
          .unsafeRunSync() shouldBe someCommitInfoExpectation
      }

    "get commit info from the configured url " +
      "and return some CommitInfo if OK returned with valid body - case with no access token" in new TestCase {

        val someCommitInfoExpectation = CommitInfo(
          id = commitId,
          message = commitMessage,
          committedDate = committedDate,
          author = author,
          committer = committer,
          parents = parents
        ).some

        setGitLabClientExpectation(maybeAccessToken, returning = someCommitInfoExpectation)

        finder
          .getMaybeCommitInfo(projectId, commitId)(maybeAccessToken)
          .unsafeRunSync() shouldBe someCommitInfoExpectation
      }
  }

  "return None if remote client responds with Not found" in new TestCase {
    mapToMaybeCommit((Status.NotFound, Request[IO](), Response[IO]())).unsafeRunSync() shouldBe None
  }

  "fallback to fetch commit info without an access token for UNAUTHORIZED" in new TestCase {

    val result = commitInfos.generateOption

    (gitLabClient
      .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[CommitInfo]])(
        _: Option[AccessToken]
      ))
      .expects(*, *, *, *, Option.empty[AccessToken])
      .returning(result.pure[IO])

    mapToMaybeCommit((Status.Unauthorized, Request[IO](), Response[IO]())).unsafeRunSync() shouldBe result
  }

  "return an Error if remote client responds with invalid json" in new TestCase {

    intercept[Exception] {
      mapToMaybeCommit(
        (Status.Ok, Request[IO](), Response[IO]().withEntity("{}").withHeaders(Header.Raw(ci"X-Next-Page", "")))
      ).unsafeRunSync()
    }
  }

  "return an Error if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

    intercept[Exception] {
      mapToMaybeCommit((Status.BadRequest, Request[IO](), Response[IO]())).unsafeRunSync()
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateSome

    val projectId     = projectIds.generateOne
    val commitId      = commitIds.generateOne
    val commitMessage = commitMessages.generateOne
    val committedDate = CommittedDate(LocalDateTime.of(2012, 9, 20, 9, 6, 12).atOffset(ZoneOffset.ofHours(3)).toInstant)
    val author        = authors.generateOne
    val committer     = committers.generateOne
    val parents       = parentsIdsLists().generateOne
    private implicit val logger: TestLogger[IO] = TestLogger()
    val gitLabClient = mock[GitLabClient[IO]]
    val finder       = new CommitInfoFinderImpl[IO](gitLabClient)

    lazy val responseJson =
      json"""{
      "id":              ${commitId.value},
      "author_name":     ${author.name.value},
      "author_email":    ${author.emailToJson},
      "committer_name":  ${committer.name.value},
      "committer_email": ${committer.emailToJson},
      "message":         ${commitMessage.value},
      "committed_date":  "2012-09-20T09:06:12+03:00",
      "parent_ids":      ${parents.map(_.value)}
    }"""

    val endpointName: String Refined NonEmpty = "commit-details"

    def setGitLabClientExpectation(maybeAccessToken: Option[AccessToken] = accessTokens.generateSome,
                                   returning:        Option[CommitInfo] = commitInfos.generateOne.some
    ) =
      (gitLabClient
        .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[CommitInfo]])(
          _: Option[AccessToken]
        ))
        .expects(GET,
                 uri"projects" / projectId.show / "repository" / "commits" / commitId.show,
                 endpointName,
                 *,
                 maybeAccessToken
        )
        .returning(returning.pure[IO])

    val mapToCommitOrThrow = captureMapping(finder, gitLabClient)(
      findingMethod = _.findCommitInfo(projectId, commitId)(maybeAccessToken = None).unsafeRunSync(),
      resultGenerator = commitInfos.generateOne
    )

    val mapToMaybeCommit = captureMapping(finder, gitLabClient)(
      findingMethod = _.getMaybeCommitInfo(projectId, commitId)(maybeAccessToken = None).unsafeRunSync(),
      resultGenerator = commitInfos.generateSome
    )
  }
}
