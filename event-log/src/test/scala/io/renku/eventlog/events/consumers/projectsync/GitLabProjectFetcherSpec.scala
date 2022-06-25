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

package io.renku.eventlog.events.consumers.projectsync

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder.Implicits._
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.EndpointTester._
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.Status.{Forbidden, InternalServerError, NotFound, Unauthorized}
import org.http4s.implicits._
import org.http4s.{Request, Response, Status, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GitLabProjectFetcherSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with should.Matchers
    with GitLabClientTools[IO] {

  "fetchGitLabProject" should {

    "fetches relevant project info from GitLab" in new TestCase {

      givenFindAccessToken(by = projectId, returning = maybeAccessToken.pure[IO])

      val projectPath = projectPaths.generateSome
      val singleProjectEndpoint: String Refined NonEmpty = "single-project"
      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(
          _: ResponseMappingF[IO, Either[UnauthorizedException, Option[projects.Path]]]
        )(_: Option[AccessToken]))
        .expects(uri"projects" / projectId.show, singleProjectEndpoint, *, maybeAccessToken)
        .returning(projectPath.asRight.pure[IO])

      fetcher.fetchGitLabProject(projectId).unsafeRunSync() shouldBe projectPath.asRight
    }

    "fail if finding access token fails" in new TestCase {
      val exception = exceptions.generateOne
      givenFindAccessToken(by = projectId, returning = exception.raiseError[IO, Option[AccessToken]])

      intercept[Exception] {
        fetcher.fetchGitLabProject(projectId).unsafeRunSync()
      } shouldBe exception
    }

    "extract path_with_namespace from the OK response from GitLab" in new TestCase {
      val projectPath = projectPaths.generateOne
      mapResponse(Status.Ok,
                  Request[IO](),
                  Response[IO](Status.Ok).withEntity(toResponseEntity(projectId, projectPath))
      ).unsafeRunSync() shouldBe projectPath.some.asRight
    }

    NotFound :: InternalServerError :: Nil foreach { status =>
      s"return None if Gitlab responds with $status" in new TestCase {
        mapResponse(status, Request[IO](), Response[IO](status)).unsafeRunSync() shouldBe None.asRight
      }
    }

    Unauthorized :: Forbidden :: Nil foreach { status =>
      s"return UnauthorizedException if Gitlab responds with $status" in new TestCase {
        mapResponse(status, Request[IO](), Response[IO](status)).unsafeRunSync() shouldBe UnauthorizedException.asLeft
      }
    }

    "fail if Gitlab responds with other status" in new TestCase {
      val status = Status.BadRequest
      intercept[Exception] {
        mapResponse(status, Request[IO](), Response[IO](status)).unsafeRunSync()
      }.getMessage should include(status.show)
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val projectId = projectIds.generateOne

    implicit val gitLabClient:      GitLabClient[IO]      = mock[GitLabClient[IO]]
    implicit val accessTokenFinder: AccessTokenFinder[IO] = mock[AccessTokenFinder[IO]]
    val fetcher = new GitLabProjectFetcherImpl[IO]

    lazy val mapResponse = captureMapping(fetcher, gitLabClient)(
      fetcher => {
        givenFindAccessToken(by = projectId, returning = maybeAccessToken.pure[IO])
        fetcher.fetchGitLabProject(projectId).unsafeRunSync()
      },
      projectPaths.generateOption.asRight[UnauthorizedException]
    )

    def givenFindAccessToken(by: projects.Id, returning: IO[Option[AccessToken]]) =
      (accessTokenFinder
        .findAccessToken(_: projects.Id)(_: projects.Id => String))
        .expects(by, projectIdToPath)
        .returning(returning)
  }

  private def toResponseEntity(id: projects.Id, path: projects.Path): Json = json"""{
    "id": ${id.value},
    "path_with_namespace": ${path.value}
  }"""
}
