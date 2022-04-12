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

package io.renku.commiteventservice.events.categories.commitsync.eventgeneration

import cats.effect.IO
import cats.implicits.toShow
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.renku.commiteventservice.events.categories.common.CommitInfo
import io.renku.commiteventservice.events.categories.common.Generators._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.Method.GET
import org.http4s.implicits._
import org.http4s.{Header, Method, Request, Response, Status, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.ci._

class LatestCommitFinderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with ExternalServiceStubbing
    with should.Matchers
    with GitLabClientTools[IO] {

  "findLatestCommit" should {

    "return latest Commit info if remote responds with OK and valid body - access token case" in new TestCase {
      val maybePersonalAccessToken = accessTokens.generateOption

      setGitLabClientExpectation(maybePersonalAccessToken, returning = Some(commitInfo))

      latestCommitFinder.findLatestCommit(projectId)(maybePersonalAccessToken).unsafeRunSync() shouldBe
        Some(commitInfo)
    }

    "return latest Commit info if remote responds with OK and valid body - no token case" in new TestCase {

      setGitLabClientExpectation(maybeAccessToken = None)

      latestCommitFinder.findLatestCommit(projectId)(maybeAccessToken = None).unsafeRunSync() shouldBe
        Some(commitInfo)
    }

    "responseMapping: return None if remote responds with OK and no commits" in new TestCase {

      mapResponse(
        (Status.Ok, Request[IO](), Response[IO]().withEntity(json"""[]""").withHeaders(Header.Raw(ci"X-Next-Page", "")))
      ).unsafeRunSync() shouldBe None
    }

    Status.NotFound :: Status.InternalServerError :: Nil foreach { status =>
      s"return None if remote responds with $status" in new TestCase {
        mapResponse((status, Request[IO](), Response[IO]())).unsafeRunSync() shouldBe None
      }
    }

    Status.Unauthorized :: Status.Forbidden :: Nil foreach { status =>
      s"fallback to fetching the latest commit without an access token for $status" in new TestCase {
        (gitLabClient
          .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[CommitInfo]])(
            _: Option[AccessToken]
          ))
          .expects(*, *, *, *, *)
          .returning(None.pure[IO])

        mapResponse((status, Request[IO](), Response[IO]())).unsafeRunSync() shouldBe None
      }
    }

    "return an Exception if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {
      intercept[Exception] {
        mapResponse((Status.ServiceUnavailable, Request[IO](), Response[IO]())).unsafeRunSync()
      }
    }

    "return an Exception if remote client responds with unexpected body" in new TestCase {
      intercept[Exception] {
        mapResponse(
          (Status.Ok,
           Request[IO](),
           Response[IO]().withEntity(json"""{}""").withHeaders(Header.Raw(ci"X-Next-Page", ""))
          )
        ).unsafeRunSync()
      }
    }
  }

  private trait TestCase {
    val projectId  = projectIds.generateOne
    val commitInfo = commitInfos.generateOne
    private implicit val logger: TestLogger[IO] = TestLogger()
    val gitLabClient       = mock[GitLabClient[IO]]
    val latestCommitFinder = new LatestCommitFinderImpl[IO](gitLabClient)

    val endpointName: String Refined NonEmpty = "commits"

    def setGitLabClientExpectation(maybeAccessToken: Option[AccessToken] = accessTokens.generateSome,
                                   returning:        Option[CommitInfo] = Some(commitInfo)
    ) = (gitLabClient
      .send(_: Method, _: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[CommitInfo]])(
        _: Option[AccessToken]
      ))
      .expects(GET,
               uri"projects" / projectId.show / "repository" / "commits" withQueryParam ("per_page", "1"),
               endpointName,
               *,
               maybeAccessToken
      )
      .returning(returning.pure[IO])

    val mapResponse: ResponseMappingF[IO, Option[CommitInfo]] = captureMapping(latestCommitFinder, gitLabClient)(
      _.findLatestCommit(projectId)(accessTokens.generateOption).unsafeRunSync(),
      commitInfos.toGeneratorOfOptions
    )
  }
}
