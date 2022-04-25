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

package io.renku.webhookservice.hookcreation.project

import cats.effect.IO
import cats.syntax.all._
import com.github.tomakehurst.wiremock.client.WireMock._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects.Visibility
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import org.http4s.circe.jsonEncoder
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{Request, Response, Status, Uri}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectInfoFinderSpec
    extends AnyWordSpec
    with MockFactory
    with ExternalServiceStubbing
    with GitLabClientTools[IO]
    with should.Matchers
    with IOSpec {

  "findProjectInfo" should {

    "return fetched project info if service responds with 200 and a valid body" in new TestCase {

      implicit override val maybeAccessToken: Option[AccessToken] = accessTokens.generateSome

      (gitLabClient
        .get(_: Uri, _: NES)(_: ResponseMappingF[IO, ProjectInfo])(_: Option[AccessToken]))
        .expects(uri, endpointName, *, maybeAccessToken)
        .returning(projectInfo.pure[IO])

      projectInfoFinder.findProjectInfo(projectId).unsafeRunSync() shouldBe ProjectInfo(
        projectId,
        projectVisibility,
        projectPath
      )
    }

    "return fetched public project info if service responds with 200 and a valid body - no token case" in new TestCase {
      implicit override val maybeAccessToken: Option[AccessToken] = None
      override val projectInfo: ProjectInfo = ProjectInfo(projectId, Visibility.Public, projectPath)

      (gitLabClient
        .get(_: Uri, _: NES)(_: ResponseMappingF[IO, ProjectInfo])(_: Option[AccessToken]))
        .expects(uri, endpointName, *, maybeAccessToken)
        .returning(projectInfo.pure[IO])

      projectInfoFinder.findProjectInfo(projectId).unsafeRunSync() shouldBe ProjectInfo(
        projectId,
        Visibility.Public,
        projectPath
      )
    }

    // mapResponse tests
    "return project info if gitLabClient returns Ok" in new TestCase {

      mapResponse(Status.Ok, Request(), Response().withEntity(projectJson(accessTokens.generateSome)))
        .unsafeRunSync() shouldBe projectInfo
    }

    "return an UnauthorizedException if remote client responds with UNAUTHORIZED" in new TestCase {

      intercept[Exception] {
        mapResponse(Status.Unauthorized, Request(), Response()).unsafeRunSync()
      } shouldBe UnauthorizedException
    }

    "return a RuntimeException if remote client responds with status neither OK nor UNAUTHORIZED" in new TestCase {

      intercept[Exception] {
        mapResponse(Status.NotFound, Request(), Response()).unsafeRunSync()
      } shouldBe a[RuntimeException]
    }

    "return a RuntimeException if remote client responds with unexpected body" in new TestCase {

      stubFor {
        get(s"/api/v4/projects/$projectId")
          .willReturn(okJson("{}"))
      }

      intercept[Exception] {
        mapResponse(Status.Ok, Request(), Response().withEntity(json"{}")).unsafeRunSync()
      }.getMessage should startWith("Invalid message body: Could not decode JSON")
    }
  }

  private trait TestCase {
    type NES = String Refined NonEmpty

    val gitLabUrl         = GitLabUrl(externalServiceBaseUrl)
    val projectId         = projectIds.generateOne
    val projectVisibility = projectVisibilities.generateOne
    val projectPath       = projectPaths.generateOne
    val projectInfo: ProjectInfo = ProjectInfo(projectId, projectVisibility, projectPath)

    val uri:          Uri = uri"projects" / projectId.show
    val endpointName: NES = "project"

    implicit val logger:           TestLogger[IO]      = TestLogger[IO]()
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    val gitLabClient      = mock[GitLabClient[IO]]
    val projectInfoFinder = new ProjectInfoFinderImpl[IO](gitLabClient)

    def projectJson(maybeAccessToken: Option[AccessToken]): String = maybeAccessToken match {
      case Some(_) =>
        json"""{
          "id": ${projectId.value},
          "visibility": ${projectVisibility.value},
          "path_with_namespace": ${projectPath.value}
        }""".noSpaces
      case None =>
        json"""{
          "id": ${projectId.value},
          "path_with_namespace": ${projectPath.value}
        }""".noSpaces
    }

    lazy val mapResponse = captureMapping(projectInfoFinder, gitLabClient)(
      _.findProjectInfo(projectId).unsafeRunSync(),
      Gen.const(projectInfo)
    )
  }
}
