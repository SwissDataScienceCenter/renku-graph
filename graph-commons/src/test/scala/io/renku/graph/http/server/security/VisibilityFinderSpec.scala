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

package io.renku.graph.http.server.security

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators._
import io.renku.graph.model.projects
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestLogger
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.implicits._
import org.http4s.{Request, Response, Status, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class VisibilityFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with MockFactory
    with GitLabClientTools[IO] {

  "findVisibility" should {

    "return project visibility if found" in new TestCase {

      val visibility = projectVisibilities.generateSome
      givenFindingVisibility(returning = visibility.pure[IO])

      finder.findVisibility(projectSlug).unsafeRunSync() shouldBe visibility
    }

    "return no visibility if not found" in new TestCase {

      givenFindingVisibility(returning = None.pure[IO])

      finder.findVisibility(projectSlug).unsafeRunSync() shouldBe None
    }

    "return visibility if OK and relevant property exists in the response" in new TestCase {

      val visibility = projectVisibilities.generateOne

      mapResponse(Status.Ok, Request(), Response().withEntity(visibility.asJson(encoder)))
        .unsafeRunSync() shouldBe visibility.some
    }

    "return no visibility if OK and relevant property does not exist in the response" in new TestCase {
      mapResponse(Status.Ok, Request(), Response().withEntity(Json.obj()))
        .unsafeRunSync() shouldBe None
    }

    Status.NotFound :: Status.Unauthorized :: Status.Forbidden :: Nil foreach { status =>
      s"return no visibility for $status" in new TestCase {
        mapResponse(status, Request(), Response()).unsafeRunSync() shouldBe None
      }
    }
  }

  private trait TestCase {

    val projectSlug = projectSlugs.generateOne
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    private implicit val logger: TestLogger[IO]   = TestLogger[IO]()
    implicit val gitLabClient:   GitLabClient[IO] = mock[GitLabClient[IO]]
    val finder = new VisibilityFinderImpl[IO]

    def givenFindingVisibility(returning: IO[Option[projects.Visibility]]) = {
      val endpointName: String Refined NonEmpty = "single-project"
      val uri = uri"projects" / projectSlug

      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[projects.Visibility]])(
          _: Option[AccessToken]
        ))
        .expects(uri, endpointName, *, maybeAccessToken)
        .returning(returning)
    }

    val mapResponse =
      captureMapping(gitLabClient)(
        finder.findVisibility(projectSlug)(maybeAccessToken).unsafeRunSync(),
        projectVisibilities.generateOption,
        underlyingMethod = Get
      )
  }

  private lazy val encoder: Encoder[projects.Visibility] = Encoder.instance { visibility =>
    json"""{"visibility": $visibility}"""
  }
}
