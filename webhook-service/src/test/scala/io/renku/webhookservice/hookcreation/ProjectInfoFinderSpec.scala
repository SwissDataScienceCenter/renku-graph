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

package io.renku.webhookservice.hookcreation

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{GitLabClientTools, IOSpec}
import io.renku.webhookservice.WebhookServiceGenerators.projects
import io.renku.webhookservice.model.Project
import org.http4s.{Request, Response, Status, Uri}
import org.http4s.circe.jsonEncoder
import org.http4s.implicits.http4sLiteralsSyntax
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
        .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Project])(_: Option[AccessToken]))
        .expects(uri, endpointName, *, maybeAccessToken)
        .returning(project.pure[IO])

      projectInfoFinder.findProjectInfo(projectId).unsafeRunSync() shouldBe project
    }

    "return project info if gitLabClient returns Ok" in new TestCase {
      mapResponse(Status.Ok, Request(), Response().withEntity(projectJson))
        .unsafeRunSync() shouldBe project
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
      intercept[Exception] {
        mapResponse(Status.Ok, Request(), Response().withEntity(json"{}")).unsafeRunSync()
      }.getMessage should startWith("Invalid message body: Could not decode JSON")
    }
  }

  private trait TestCase {
    val project     = projects.generateOne
    val projectId   = project.id
    val projectPath = project.path

    val uri:          Uri                     = uri"projects" / projectId.show
    val endpointName: String Refined NonEmpty = "single-project"

    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    implicit val logger:       TestLogger[IO]   = TestLogger[IO]()
    implicit val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
    val projectInfoFinder = new ProjectInfoFinderImpl[IO]

    lazy val projectJson: String = json"""{
      "id":                  $projectId,
      "path_with_namespace": $projectPath
    }""".noSpaces

    lazy val mapResponse = captureMapping(gitLabClient)(
      projectInfoFinder.findProjectInfo(projectId).unsafeRunSync(),
      Gen.const(project)
    )
  }
}
