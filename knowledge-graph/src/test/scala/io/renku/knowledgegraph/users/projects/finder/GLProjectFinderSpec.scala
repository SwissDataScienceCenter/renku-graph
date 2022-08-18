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

package io.renku.knowledgegraph.users.projects
package finder

import Endpoint.Criteria
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.personGitLabIds
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.implicits._
import org.http4s.{Request, Response, Status, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class GLProjectFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with ExternalServiceStubbing
    with IOSpec
    with MockFactory
    with GitLabClientTools[IO] {

  "findProjectsInGL" should {

    "call the GitLab User Projects API and return the results" in new TestCase {

      val criteria = criterias.generateOne

      val projectsAndCreators = notActivatedProjects
        .generateNonEmptyList()
        .toList
        .map(p => p -> p.maybeCreator.map(_ => personGitLabIds.generateOne))

      (gitLabClient
        .get(_: Uri, _: String Refined NonEmpty)(
          _: ResponseMappingF[IO, List[(model.Project.NotActivated, Option[persons.GitLabId])]]
        )(_: Option[AccessToken]))
        .expects(uri(criteria), endpointName, *, criteria.maybeUser.map(_.accessToken))
        .returning(projectsAndCreators.pure[IO])

      projectsAndCreators.foreach {
        case (project, Some(creatorId)) =>
          (glCreatorFinder
            .findCreatorName(_: persons.GitLabId)(_: Option[AccessToken]))
            .expects(creatorId, criteria.maybeUser.map(_.accessToken))
            .returning(project.maybeCreator.pure[IO])
        case (_, None) => ()
      }

      finder.findProjectsInGL(criteria).unsafeRunSync() shouldBe projectsAndCreators.map(_._1)
    }

    "map OK response from GitLab to list of NotActivated" in new TestCase {

      val project        = notActivatedProjects.generateOne.copy(visibility = projects.Visibility.Public)
      val maybeCreatorId = project.maybeCreator.map(_ => personGitLabIds.generateOne)

      mapResponse(Status.Ok, Request[IO](), Response[IO](Status.Ok).withEntity(List(project -> maybeCreatorId).asJson))
        .unsafeRunSync() shouldBe List(
        project.copy(maybeCreator = None, keywords = project.keywords.sorted) -> maybeCreatorId
      )
    }

    "map OK with an empty list to an empty list" in new TestCase {
      mapResponse(Status.Ok, Request[IO](), Response[IO](Status.Ok).withEntity(Json.arr()))
        .unsafeRunSync() shouldBe Nil
    }

    "map NOT_FOUND to an empty list" in new TestCase {
      mapResponse(Status.NotFound, Request[IO](), Response[IO](Status.NotFound)).unsafeRunSync() shouldBe Nil
    }

    "return a RuntimeException if remote client responds with status different than OK or NOT_FOUND" in new TestCase {
      intercept[Exception] {
        mapResponse(Status.Unauthorized, Request[IO](), Response[IO]())
      }
    }
  }

  private lazy val endpointName: String Refined NonEmpty = "user-projects"

  private trait TestCase {
    implicit val logger:       TestLogger[IO]   = TestLogger[IO]()
    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val glCreatorFinder = mock[GLCreatorFinder[IO]]
    val finder          = new GLProjectFinderImpl[IO](glCreatorFinder)

    lazy val mapResponse = captureMapping(finder, gitLabClient)(
      finder => {
        (glCreatorFinder
          .findCreatorName(_: persons.GitLabId)(_: Option[AccessToken]))
          .expects(*, *)
          .returning(Option.empty[persons.Name].pure[IO])
          .anyNumberOfTimes()

        val criteria = criterias.generateOne
        finder.findProjectsInGL(criteria).unsafeRunSync()
      },
      notActivatedProjects
        .map(p => p.maybeCreator.map(_ => p -> personGitLabIds.generateOption).getOrElse(p -> None))
        .toGeneratorOfList()
    )
  }

  private def uri(criteria: Criteria) = uri"users" / criteria.userId.show / "projects"

  private implicit lazy val projectEncoder: Encoder[(model.Project.NotActivated, Option[persons.GitLabId])] = {
    import io.renku.tinytypes.json.TinyTypeEncoders._
    Encoder.instance { case (project, maybeCreatorId) =>
      json"""{
        "id":                  ${project.id},
        "description":         ${project.maybeDesc.map(_.asJson).getOrElse(Json.Null)},
        "topics":              ${project.keywords},
        "name":                ${project.name},
        "path_with_namespace": ${project.path},
        "created_at":          ${project.dateCreated},
        "creator_id":          ${maybeCreatorId.map(_.asJson).getOrElse(Json.Null)}
      }"""
        .addIfDefined("visibility" -> Option.when(project.visibility != projects.Visibility.Public)(project.visibility))
    }
  }
}
