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

package io.renku.webhookservice

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.{CustomAsyncIOSpec, GitLabClientTools}
import org.http4s.circe.jsonEncoder
import org.http4s.implicits._
import org.http4s.{Request, Response, Status, Uri}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectInfoFinderSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with AsyncMockFactory
    with ExternalServiceStubbing
    with GitLabClientTools[IO]
    with should.Matchers
    with OptionValues {

  it should "return fetched project info if service responds with 200 and a valid body" in {

    val mat: Option[AccessToken] = accessTokens.generateSome

    (gitLabClient
      .get(_: Uri, _: String Refined NonEmpty)(_: ResponseMappingF[IO, Option[Project]])(_: Option[AccessToken]))
      .expects(uri, endpointName, *, mat)
      .returning(project.some.pure[IO])

    projectInfoFinder.findProjectInfo(projectId)(mat).asserting(_.value shouldBe project)
  }

  it should "decode project info from a valid OK response" in {
    mapResponse(Status.Ok, Request(), Response().withEntity(projectJson)).asserting(_.value shouldBe project)
  }

  it should "decode to none if remote returns NotFound" in {
    mapResponse(Status.NotFound, Request(), Response()).asserting(_ shouldBe None)
  }

  it should "fail with an UnauthorizedException if remote responds with UNAUTHORIZED" in {
    mapResponse(Status.Unauthorized, Request(), Response()).assertThrows[UnauthorizedException]
  }

  it should "do not define a mapping case for status different than OK, NOT_FOUND or UNAUTHORIZED" in {
    intercept[Exception](
      mapResponse(Status.InternalServerError, Request(), Response()).assertNoException
    ) shouldBe a[MatchError]
  }

  it should "return an Exception if remote client responds with unexpected body" in {
    mapResponse(Status.Ok, Request(), Response().withEntity(json"{}"))
      .assertThrowsError[Exception](_.getMessage should startWith("Invalid message body: Could not decode JSON"))
  }

  private lazy val project   = consumerProjects.generateOne
  private lazy val projectId = project.id
  private lazy val uri:          Uri                     = uri"projects" / projectId
  private lazy val endpointName: String Refined NonEmpty = "single-project"

  private implicit lazy val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

  private implicit val logger:            TestLogger[IO]   = TestLogger[IO]()
  private implicit lazy val gitLabClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val projectInfoFinder = new ProjectInfoFinderImpl[IO]

  private lazy val projectJson: String = json"""{
      "id":                  $projectId,
      "path_with_namespace": ${project.slug}
    }""".noSpaces

  private lazy val mapResponse = captureMapping(gitLabClient)(
    projectInfoFinder.findProjectInfo(projectId).unsafeRunSync(),
    consumerProjects.generateSome,
    underlyingMethod = Get
  )
}
