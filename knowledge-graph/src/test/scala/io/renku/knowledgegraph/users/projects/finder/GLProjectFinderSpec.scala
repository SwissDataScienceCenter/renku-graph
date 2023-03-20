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

package io.renku.knowledgegraph.users.projects
package finder

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.{Encoder, Json}
import io.circe.literal._
import io.circe.syntax._
import io.renku.generators.CommonGraphGenerators.totals
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.model.GraphModelGenerators.personGitLabIds
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.rest.paging.model.{Page, Total}
import io.renku.http.server.EndpointTester._
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestLogger
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.{Header, Request, Response, Status, Uri}
import org.http4s.implicits._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.typelevel.ci._

class GLProjectFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with MockFactory
    with GitLabClientTools[IO] {

  private type ResultsChunk = (List[model.Project.NotActivated], Option[Total])

  "findProjectsInGL" should {

    "call the GitLab Projects API with membership=true and min_access_level=40 and return the results " +
      "if there's a single page of results" in new TestCase {

        val criteria = criterias.generateOne

        val projects = notActivatedProjects
          .generateNonEmptyList(max = pageSize)
          .toList

        (gitLabClient
          .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, ResultsChunk])(_: Option[AccessToken]))
          .expects(uri(Page.first), endpointName, *, criteria.maybeUser.map(_.accessToken))
          .returning((projects -> totalFrom(projects)).pure[IO])

        finder.findProjectsInGL(criteria).unsafeRunSync() shouldBe projects
      }

    "read the data from the first page and all the other pages if there are many" in new TestCase {

      val criteria = criterias.generateOne

      val projects = notActivatedProjects
        .generateNonEmptyList(min = pageSize + 1, max = pageSize * 3)
        .toList

      projects.sliding(pageSize, pageSize).zipWithIndex foreach { case (resultsPage, idx) =>
        val currentPage = Page(idx + 1)
        (gitLabClient
          .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, ResultsChunk])(_: Option[AccessToken]))
          .expects(uri(currentPage), endpointName, *, criteria.maybeUser.map(_.accessToken))
          .returning((resultsPage -> totalFrom(projects)).pure[IO])
      }

      finder.findProjectsInGL(criteria).unsafeRunSync() shouldBe projects
    }

    "map OK response from GitLab to list of NotActivated" in new TestCase {

      val project = notActivatedProjects.generateOne.copy(visibility = projects.Visibility.Public)
      val total   = totals.generateOne

      val response = Response[IO](Status.Ok)
        .withEntity(List(project).asJson)
        .withHeaders(Header.Raw(ci"X-Total-Pages", total.show))

      val expected = List(project.copy(maybeCreator = None, keywords = project.keywords.sorted)) -> total.some

      mapResponse(Status.Ok, Request[IO](), response).unsafeRunSync() shouldBe expected
    }

    "map OK with an empty list to an empty list" in new TestCase {
      mapResponse(Status.Ok, Request[IO](), Response[IO](Status.Ok).withEntity(Json.arr()))
        .unsafeRunSync() shouldBe Nil -> None
    }

    "map NOT_FOUND to an empty list" in new TestCase {
      mapResponse(Status.NotFound, Request[IO](), Response[IO](Status.NotFound)).unsafeRunSync() shouldBe Nil -> None
    }

    "return a RuntimeException if remote client responds with status different than OK or NOT_FOUND" in new TestCase {
      intercept[Exception] {
        mapResponse(Status.Unauthorized, Request[IO](), Response[IO]())
      }
    }
  }

  private lazy val endpointName: String Refined NonEmpty = "user-projects"

  private trait TestCase {
    val pageSize = GLProjectFinder.requestPageSize.value
    implicit val logger:       TestLogger[IO]   = TestLogger[IO]()
    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val finder = new GLProjectFinderImpl[IO]

    lazy val mapResponse = captureMapping(gitLabClient)(
      finder.findProjectsInGL(criterias.generateOne).unsafeRunSync(),
      (notActivatedProjects
         .map(p => p.maybeCreator.map(_ => p -> personGitLabIds.generateOption).getOrElse(p -> Total(1).some))
         .toGeneratorOfList(),
       fixed(Option(Total(1)))
      ).mapN(_ -> _)
    )

    def totalFrom(projects: List[model.Project.NotActivated]) =
      Total(projects.size / pageSize + (if ((projects.size % pageSize) == 0) 0 else 1)).some
  }

  private def uri(page: Page) =
    uri"projects"
      .withQueryParam("membership", true)
      .withQueryParam("min_access_level", 40)
      .withQueryParam("page", page)
      .withQueryParam("per_page", GLProjectFinder.requestPageSize)

  private implicit lazy val projectEncoder: Encoder[model.Project.NotActivated] = Encoder.instance { project =>
    json"""{
      "id":                  ${project.id},
      "description":         ${project.maybeDesc.map(_.asJson).getOrElse(Json.Null)},
      "topics":              ${project.keywords},
      "name":                ${project.name},
      "path_with_namespace": ${project.path},
      "created_at":          ${project.dateCreated},
      "creator_id":          ${project.maybeCreatorId.map(_.asJson).getOrElse(Json.Null)}
    }"""
      .addIfDefined("visibility" -> Option.when(project.visibility != projects.Visibility.Public)(project.visibility))
  }
}
