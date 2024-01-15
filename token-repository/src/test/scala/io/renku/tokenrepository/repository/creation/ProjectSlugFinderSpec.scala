/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository.creation

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
import io.renku.http.RenkuEntityCodec
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestLogger
import io.renku.testtools.{CustomAsyncIOSpec, GitLabClientTools}
import org.http4s.implicits._
import org.http4s.{Request, Response, Status, Uri}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks

class ProjectSlugFinderSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with AsyncMockFactory
    with GitLabClientTools[IO]
    with TableDrivenPropertyChecks
    with RenkuEntityCodec {

  forAll {
    Table(
      "Token type"              -> "token",
      "Project Access Token"    -> projectAccessTokens.generateOne,
      "User OAuth Access Token" -> userOAuthAccessTokens.generateOne,
      "Personal Access Token"   -> personalAccessTokens.generateOne
    )
  } { (tokenType, accessToken: AccessToken) =>
    it should s"return fetched Project's slug if service responds with OK and a valid body - case when $tokenType given" in {

      val projectId   = projectIds.generateOne
      val projectSlug = projectSlugs.generateOne

      val endpointName: String Refined NonEmpty = "single-project"
      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[projects.Slug]])(
          _: Option[AccessToken]
        ))
        .expects(uri"projects" / projectId, endpointName, *, Option(accessToken))
        .returning(projectSlug.some.pure[IO])

      slugFinder.findProjectSlug(projectId, accessToken).asserting(_ shouldBe projectSlug.some)
    }
  }

  it should "map OK response body to project slug" in {

    val projectId   = projectIds.generateOne
    val projectSlug = projectSlugs.generateOne

    mapResponse(Status.Ok, Request[IO](), Response[IO](Status.Ok).withEntity(projectJson(projectId, projectSlug)))
      .asserting(_ shouldBe projectSlug.some)
  }

  Status.Unauthorized :: Status.Forbidden :: Status.NotFound :: Nil foreach { status =>
    it should s"map $status response to None" in {
      mapResponse(status, Request[IO](), Response[IO](status)).asserting(_ shouldBe None)
    }
  }

  it should "map UNAUTHORIZED response to None" in {
    mapResponse(Status.Unauthorized, Request[IO](), Response[IO](Status.Unauthorized)).asserting(_ shouldBe None)
  }

  it should "throws a MatchError if remote responds with status different than OK, NOT_FOUND or UNAUTHORIZED" in {
    IO(mapResponse(Status.BadRequest, Request[IO](), Response[IO](Status.BadRequest))).flatten.assertThrows[MatchError]
  }

  it should "return an Exception if remote client responds with unexpected body" in {
    mapResponse(Status.Ok, Request[IO](), Response[IO](Status.Ok).withEntity(Json.obj())).assertThrows[Exception]
  }

  private implicit val logger:       TestLogger[IO]   = TestLogger[IO]()
  private implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val slugFinder = new ProjectSlugFinderImpl[IO]

  private def projectJson(projectId: projects.GitLabId, projectSlug: projects.Slug) = json"""{
      "id":                  $projectId,
      "path_with_namespace": $projectSlug
    }"""

  private lazy val mapResponse = captureMapping(gitLabClient)(
    findingMethod = slugFinder.findProjectSlug(projectIds.generateOne, accessTokens.generateOne).unsafeRunSync(),
    resultGenerator = projectSlugs.generateOption,
    underlyingMethod = Get
  )
}
