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

package io.renku.knowledgegraph.projects.update

import Generators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{jsons, nonEmptyStrings}
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.projects
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.testtools.GitLabClientTools
import org.http4s.Method.PUT
import org.http4s.Status.{BadRequest, Ok}
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.implicits._
import org.http4s.{Request, Response, Uri, UrlForm}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class GLProjectUpdaterSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with EitherValues
    with GitLabClientTools[IO] {

  it should s"call GL's PUT gl/projects/:slug and return updated values on success" in {

    val slug           = projectSlugs.generateOne
    val newValues      = projectUpdatesGen.suchThat(_.glUpdateNeeded).generateOne
    val accessToken    = accessTokens.generateOne
    val updatedProject = glUpdatedProjectsGen.generateOne

    givenEditProjectAPICall(slug, newValues, accessToken, returning = updatedProject.asRight.pure[IO])

    finder.updateProject(slug, newValues, accessToken).asserting(_.value shouldBe updatedProject.some)
  }

  it should s"do nothing if neither new image nor visibility is set in the update" in {

    val slug        = projectSlugs.generateOne
    val newValues   = projectUpdatesGen.generateOne.copy(newImage = None, newVisibility = None)
    val accessToken = accessTokens.generateOne

    finder.updateProject(slug, newValues, accessToken).asserting(_.value shouldBe None)
  }

  it should s"call GL's PUT gl/projects/:slug and return GL message if returned" in {

    val slug        = projectSlugs.generateOne
    val newValues   = projectUpdatesGen.suchThat(u => u.newImage.orElse(u.newVisibility).isDefined).generateOne
    val accessToken = accessTokens.generateOne

    val error = Message.Error.fromJsonUnsafe(jsons.generateOne)
    givenEditProjectAPICall(slug, newValues, accessToken, returning = error.asLeft.pure[IO])

    finder
      .updateProject(slug, newValues, accessToken)
      .asserting(_.left.value shouldBe error)
  }

  it should "succeed and return updated values if PUT gl/projects/:slug returns 200 OK" in {
    val updatedProject = glUpdatedProjectsGen.generateOne
    mapResponse(Ok, Request[IO](), Response[IO]().withEntity(updatedProject.asJson))
      .asserting(_.value shouldBe updatedProject)
  }

  it should "return left if PUT gl/projects/:slug returns 400 BAD_REQUEST with an error" in {

    val error = nonEmptyStrings().generateOne

    mapResponse(BadRequest, Request[IO](), Response[IO](BadRequest).withEntity(json"""{"error": $error}"""))
      .asserting(_.left.value shouldBe Message.Error.fromJsonUnsafe(Json.fromString(error)))
  }

  it should "return left if PUT gl/projects/:slug returns 400 BAD_REQUEST with a message" in {

    val message = jsons.generateOne

    mapResponse(BadRequest, Request[IO](), Response[IO](BadRequest).withEntity(json"""{"message": $message}"""))
      .asserting(_.left.value shouldBe Message.Error.fromJsonUnsafe(message))
  }

  private implicit val glClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val finder = new GLProjectUpdaterImpl[IO]

  private def givenEditProjectAPICall(slug:        projects.Slug,
                                      newValues:   ProjectUpdates,
                                      accessToken: AccessToken,
                                      returning:   IO[Either[Message, GLUpdatedProject]]
  ) = {
    val endpointName: String Refined NonEmpty = "edit-project"
    (glClient
      .put(_: Uri, _: String Refined NonEmpty, _: UrlForm)(_: ResponseMappingF[IO, Either[Message, GLUpdatedProject]])(
        _: Option[AccessToken]
      ))
      .expects(uri"projects" / slug, endpointName, toUrlForm(newValues), *, accessToken.some)
      .returning(returning)
  }

  private def toUrlForm: ProjectUpdates => UrlForm = { case ProjectUpdates(_, newImage, _, newVisibility) =>
    UrlForm(
      List(
        newImage.map("avatar" -> _.fold[String](null)(_.value)),
        newVisibility.map("visibility" -> _.value)
      ).flatten: _*
    )
  }

  private lazy val mapResponse: ResponseMappingF[IO, Either[Message, GLUpdatedProject]] =
    captureMapping(glClient)(
      finder
        .updateProject(projectSlugs.generateOne,
                       projectUpdatesGen.suchThat(u => u.newImage.orElse(u.newVisibility).isDefined).generateOne,
                       accessTokens.generateOne
        )
        .unsafeRunSync(),
      glUpdatedProjectsGen.generateOne.asRight[Message],
      method = PUT
    )

  private implicit lazy val responseEncoder: Encoder[GLUpdatedProject] = Encoder.instance {
    case GLUpdatedProject(image, visibility) =>
      json"""{
        "avatar_url": $image,
        "visibility": $visibility
      }"""
  }
}
