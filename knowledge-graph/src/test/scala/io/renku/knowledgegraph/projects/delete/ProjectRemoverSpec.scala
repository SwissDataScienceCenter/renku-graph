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

package io.renku.knowledgegraph.projects.delete

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.http.client.GitLabGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectIds
import io.renku.graph.model.projects
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.Status._
import org.http4s.implicits._
import org.http4s.{Request, Response, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectRemoverSpec
    extends AnyWordSpec
    with should.Matchers
    with MockFactory
    with IOSpec
    with GitLabClientTools[IO] {

  "deleteProject" should {

    "call the Delete Project GL API" in new TestCase {

      givenDeleteAPICall(id, returning = ().pure[IO])

      remover.deleteProject(id).unsafeRunSync() shouldBe ()
    }

    Set(Ok, Accepted, NoContent, NotFound) foreach { status =>
      s"succeed if service responds with $status" in new TestCase {
        mapResponse(status, Request[IO](), Response[IO]()).unsafeRunSync() shouldBe ()
      }
    }

    "fail for other response statuses" in new TestCase {
      intercept[Exception] {
        mapResponse(BadRequest, Request[IO](), Response[IO]()).unsafeRunSync()
      }
    }
  }

  private trait TestCase {

    implicit val accessToken: AccessToken = accessTokens.generateOne
    val id = projectIds.generateOne

    private implicit val glClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val remover = new ProjectRemoverImpl[IO]

    def givenDeleteAPICall(id: projects.GitLabId, returning: IO[Unit]) = {
      val endpointName: String Refined NonEmpty = "project-delete"
      (glClient
        .delete(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Unit])(_: Option[AccessToken]))
        .expects(uri"projects" / id, endpointName, *, accessToken.some)
        .returning(returning)
    }

    lazy val mapResponse: ResponseMappingF[IO, Unit] =
      captureMapping(glClient)(remover.deleteProject(projectIds.generateOne).unsafeRunSync(),
                               (),
                               underlyingMethod = Delete
      )
  }
}
