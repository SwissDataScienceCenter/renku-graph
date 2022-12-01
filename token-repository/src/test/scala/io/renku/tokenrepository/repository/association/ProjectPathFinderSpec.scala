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

package io.renku.tokenrepository.repository.association

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.{accessTokens, personalAccessTokens, projectAccessTokens, userOAuthAccessTokens}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.implicits._
import org.http4s.{Request, Response, Status, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class ProjectPathFinderSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with MockFactory
    with GitLabClientTools[IO]
    with TableDrivenPropertyChecks {

  "findProjectPath" should {

    forAll {
      Table(
        "Token type"              -> "token",
        "Project Access Token"    -> projectAccessTokens.generateOne,
        "User OAuth Access Token" -> userOAuthAccessTokens.generateOne,
        "Personal Access Token"   -> personalAccessTokens.generateOne
      )
    } { (tokenType, accessToken: AccessToken) =>
      s"return fetched Project's path if service responds with OK and a valid body - case when $tokenType given" in new TestCase {

        val endpointName: String Refined NonEmpty = "single-project"
        (gitLabClient
          .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[projects.Path]])(
            _: Option[AccessToken]
          ))
          .expects(uri"projects" / projectId.value, endpointName, *, Option(accessToken))
          .returning(projectPath.some.pure[IO])

        pathFinder.findProjectPath(projectId, accessToken).value.unsafeRunSync() shouldBe projectPath.some
      }
    }

    "map OK response body to project path" in new TestCase {
      mapResponse(Status.Ok, Request[IO](), Response[IO](Status.Ok).withEntity(projectJson))
        .unsafeRunSync() shouldBe projectPath.some
    }

    Status.Unauthorized :: Status.Forbidden :: Status.NotFound :: Nil foreach { status =>
      s"map $status response to None" in new TestCase {
        mapResponse(status, Request[IO](), Response[IO](status)).unsafeRunSync() shouldBe None
      }
    }

    "map UNAUTHORIZED response to None" in new TestCase {
      mapResponse(Status.Unauthorized, Request[IO](), Response[IO](Status.Unauthorized)).unsafeRunSync() shouldBe None
    }

    "return an Exception if remote responds with status different than OK, NOT_FOUND or UNAUTHORIZED" in new TestCase {
      intercept[Exception] {
        mapResponse(Status.BadRequest, Request[IO](), Response[IO](Status.BadRequest)).unsafeRunSync()
      }
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {
      intercept[Exception] {
        mapResponse(Status.Ok, Request[IO](), Response[IO](Status.Ok).withEntity(Json.obj())).unsafeRunSync()
      }
    }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    private implicit val logger: TestLogger[IO]   = TestLogger[IO]()
    implicit val gitLabClient:   GitLabClient[IO] = mock[GitLabClient[IO]]
    val pathFinder = new ProjectPathFinderImpl[IO]

    lazy val projectJson = json"""{
      "id": ${projectId.value},
      "path_with_namespace": ${projectPath.value}
    }"""

    lazy val mapResponse = captureMapping(pathFinder, gitLabClient)(
      findingMethod = _.findProjectPath(projectId, accessTokens.generateOne).value.unsafeRunSync(),
      resultGenerator = projectPaths.generateOption
    )
  }
}
