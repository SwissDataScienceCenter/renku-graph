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

package io.renku.knowledgegraph.projects.rest

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.projects.model.Permissions
import io.renku.knowledgegraph.projects.model.Permissions._
import io.renku.knowledgegraph.projects.rest.GitLabProjectFinder.GitLabProject
import io.renku.knowledgegraph.projects.rest.ProjectsGenerators._
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import io.renku.tinytypes.json.TinyTypeEncoders._
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Request, Response, Status, Uri}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class GitLabProjectFinderSpec
    extends AnyWordSpec
    with ExternalServiceStubbing
    with ScalaCheckPropertyChecks
    with should.Matchers
    with IOSpec
    with MockFactory
    with GitLabClientTools[IO] {

  "findProject" should {

    "return fetched project info if service responds with OK and a valid body" in new TestCase {
      forAll { (path: model.projects.Path, accessToken: AccessToken, project: GitLabProject) =>
        val expectation = project.some

        (gitLabClient
          .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[GitLabProject]])(
            _: Option[AccessToken]
          ))
          .expects(uri(path), endpointName, *, accessToken.some)
          .returning(expectation.pure[IO])

        projectFinder.findProject(path)(accessToken).unsafeRunSync() shouldBe expectation
      }
    }

    "return fetched project info with no readme if readme_url in remote is blank" in new TestCase {
      val gitLabProject = gitLabProjects.generateOne
      val project       = gitLabProject.copy(urls = gitLabProject.urls.copy(maybeReadme = None))

      mapResponse(Status.Ok, Request[IO](), Response[IO]().withEntity(projectJson(project)))
        .unsafeRunSync() shouldBe project.some
    }

    "return None if service responds with NOT_FOUND" in new TestCase {
      mapResponse(Status.NotFound, Request[IO](), Response[IO]()).unsafeRunSync() shouldBe None
    }

    "return a RuntimeException if remote client responds with status different than OK or NOT_FOUND" in new TestCase {

      intercept[Exception] {
        mapResponse(Status.Unauthorized, Request[IO](), Response[IO]())
      }
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {

      intercept[Exception] {
        mapResponse((Status.Ok, Request[IO](), Response[IO]().withEntity(json"""{}"""))).unsafeRunSync()
      }.getMessage should startWith(s"Invalid message body: Could not decode JSON:")
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val gitLabClient  = mock[GitLabClient[IO]]
    val projectFinder = new GitLabProjectFinderImpl[IO](gitLabClient)

    def uri(path: model.projects.Path) = uri"projects" / path.show withQueryParam ("statistics", "true")

    val endpointName:         String Refined NonEmpty = "single-project"
    implicit val accessToken: AccessToken             = accessTokens.generateOne

    val mapResponse: ResponseMappingF[IO, Option[GitLabProject]] =
      captureMapping(projectFinder, gitLabClient)(_.findProject(projectPaths.generateOne).unsafeRunSync(),
                                                  gitLabProjects.toGeneratorOfOptions
      )
  }

  private def projectJson(project: GitLabProject): Json =
    json"""{
    "id":               ${project.id},
    "visibility":       ${project.visibility},
    "ssh_url_to_repo":  ${project.urls.ssh},
    "http_url_to_repo": ${project.urls.http},
    "web_url":          ${project.urls.web},
    "forks_count":      ${project.forksCount},
    "star_count":       ${project.starsCount},
    "last_activity_at": ${project.updatedAt},
    "permissions":      ${toJson(project.permissions)},
    "statistics": {
      "commit_count":       ${project.statistics.commitsCount},
      "storage_size":       ${project.statistics.storageSize},
      "repository_size":    ${project.statistics.repositorySize},
      "lfs_objects_size":   ${project.statistics.lsfObjectsSize},
      "job_artifacts_size": ${project.statistics.jobArtifactsSize}
    }
  }""".addIfDefined("readme_url" -> project.urls.maybeReadme)

  private lazy val toJson: Permissions => Json = {
    case ProjectAndGroupPermissions(project, group) => json"""{
      "project_access": ${toJson(project)},
      "group_access":   ${toJson(group)}
    }"""
    case ProjectPermissions(project) => json"""{
      "project_access": ${toJson(project)},
      "group_access":   ${Json.Null}
    }"""
    case GroupPermissions(group) => json"""{
      "project_access": ${Json.Null},
      "group_access":   ${toJson(group)}
    }"""
  }

  private def toJson(accessLevel: AccessLevel): Json =
    json"""{
    "access_level": ${accessLevel.value.value}
  }"""
}
