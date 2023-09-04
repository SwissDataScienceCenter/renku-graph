/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.{CustomAsyncIOSpec, GitLabClientTools}
import org.http4s.circe._
import org.http4s.implicits._
import org.http4s.{Request, Response, Status, Uri}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class GLProjectVisibilityFinderSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with AsyncMockFactory
    with GitLabClientTools[IO] {

  it should s"return fetched Project's visibility if GL responds with OK and a valid body" in {

    val projectSlug = projectSlugs.generateOne
    implicit val mat: Option[AccessToken] = accessTokens.generateOption

    val maybeVisibility = projectVisibilities.generateOption
    val endpointName: String Refined NonEmpty = "single-project"
    (gitLabClient
      .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[projects.Visibility]])(
        _: Option[AccessToken]
      ))
      .expects(uri"projects" / projectSlug, endpointName, *, mat)
      .returning(maybeVisibility.pure[IO])

    visibilityFinder.findVisibility(projectSlug).asserting(_ shouldBe maybeVisibility)
  }

  it should "map OK response body to project visibility" in {

    val visibility = projectVisibilities.generateOne

    mapResponse(Status.Ok, Request[IO](), Response[IO](Status.Ok).withEntity(json"""{"visibility": $visibility}"""))
      .asserting(_ shouldBe visibility.some)
  }

  it should "map OK response without visibility property to none" in {
    mapResponse(Status.Ok, Request[IO](), Response[IO](Status.Ok).withEntity(Json.obj())).asserting(_ shouldBe None)
  }

  Status.Unauthorized :: Status.Forbidden :: Status.NotFound :: Nil foreach { status =>
    it should s"map $status response to None" in {
      mapResponse(status, Request[IO](), Response[IO](status)).asserting(_ shouldBe None)
    }
  }

  it should "throws a MatchError if remote responds with status different than OK, NOT_FOUND or UNAUTHORIZED" in {
    IO(mapResponse(Status.BadRequest, Request[IO](), Response[IO](Status.BadRequest))).flatten.assertThrows[MatchError]
  }

  it should "return an Exception if remote client responds with unexpected body" in {
    mapResponse(Status.Ok,
                Request[IO](),
                Response[IO](Status.Ok).withEntity(Json.obj("visibility" -> "unknown".asJson))
    )
      .assertThrows[Exception]
  }

  private implicit lazy val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val visibilityFinder = new GLProjectVisibilityFinderImpl[IO]

  private lazy val mapResponse = captureMapping(gitLabClient)(
    findingMethod =
      visibilityFinder.findVisibility(projectSlugs.generateOne)(accessTokens.generateOption).unsafeRunSync(),
    resultGenerator = projectVisibilities.generateOption
  )
}
