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
import io.circe.literal._
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.projects
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.{Request, Response, Uri}
import org.http4s.implicits._
import org.http4s.Status._
import org.http4s.circe._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GLProjectFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with MockFactory
    with IOSpec
    with GitLabClientTools[IO] {

  "findProject" should {

    "call the Single Project GL API and return the received Project" in new TestCase {

      givenSingleProjectAPICall(project.slug, returning = project.some.pure[IO])

      finder.findProject(project.slug).unsafeRunSync() shouldBe project.some
    }

    "call the Single Project GL API and return None for NotFound" in new TestCase {

      givenSingleProjectAPICall(project.slug, returning = None.pure[IO])

      finder.findProject(project.slug).unsafeRunSync() shouldBe None
    }

    "map the Ok response to a Project object" in new TestCase {
      mapResponse(Ok, Request[IO](), Response[IO]().withEntity(toJson(project))).unsafeRunSync() shouldBe project.some
    }

    "map the NotFound response to None" in new TestCase {
      mapResponse(NotFound, Request[IO](), Response[IO]()).unsafeRunSync() shouldBe None
    }

    "fail for other response statuses" in new TestCase {
      intercept[Exception] {
        mapResponse(BadRequest, Request[IO](), Response[IO]()).unsafeRunSync()
      }
    }
  }

  private trait TestCase {

    implicit val accessToken: AccessToken = accessTokens.generateOne
    val project = consumerProjects.generateOne

    private implicit val glClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val finder = new GLProjectFinderImpl[IO]

    def givenSingleProjectAPICall(slug: projects.Slug, returning: IO[Option[Project]]) = {
      val endpointName: String Refined NonEmpty = "single-project"
      (glClient
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[Project]])(_: Option[AccessToken]))
        .expects(uri"projects" / slug, endpointName, *, accessToken.some)
        .returning(returning)
    }

    lazy val mapResponse: ResponseMappingF[IO, Option[Project]] =
      captureMapping(glClient)(finder.findProject(projectSlugs.generateOne).unsafeRunSync(),
                               consumerProjects.toGeneratorOfOptions,
                               underlyingMethod = Get
      )
  }

  private def toJson(project: Project) = json"""{
    "id":                  ${project.id},
    "path_with_namespace": ${project.slug}
  }"""
}
