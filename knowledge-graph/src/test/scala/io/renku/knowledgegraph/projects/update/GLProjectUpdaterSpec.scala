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
import org.http4s.Status.{BadRequest, Forbidden, Ok}
import org.http4s.circe.jsonEncoder
import org.http4s.implicits._
import org.http4s.multipart.Multipart
import org.http4s.{Request, Response, Uri}
import org.scalamock.matchers.ArgCapture.CaptureOne
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatest.{EitherValues, OptionValues, Succeeded}

class GLProjectUpdaterSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with EitherValues
    with OptionValues
    with GitLabClientTools[IO] {

  it should s"call GL's PUT gl/projects/:slug and return updated values on success" in {

    val slug           = projectSlugs.generateOne
    val newValues      = projectUpdatesGen.suchThat(_.glUpdateNeeded).generateOne
    val accessToken    = accessTokens.generateOne
    val updatedProject = glUpdatedProjectsGen.generateOne

    val multipartCaptor = givenEditProjectAPICall(slug, accessToken, returning = updatedProject.asRight.pure[IO])

    finder
      .updateProject(slug, newValues, accessToken)
      .asserting(_.value shouldBe updatedProject.some)
      .flatMap(_ => verifyRequest(multipartCaptor, newValues))
  }

  it should s"do nothing if neither new image nor visibility is set in the update" in {

    val slug        = projectSlugs.generateOne
    val newValues   = projectUpdatesGen.generateOne.copy(newImage = None, newVisibility = None)
    val accessToken = accessTokens.generateOne

    finder.updateProject(slug, newValues, accessToken).asserting(_.value shouldBe None)
  }

  it should s"call GL's PUT gl/projects/:slug and return GL message if returned" in {

    val slug        = projectSlugs.generateOne
    val newValues   = projectUpdatesGen.suchThat(u => u.glUpdateNeeded).generateOne
    val accessToken = accessTokens.generateOne

    val error = Message.Error.fromJsonUnsafe(jsons.generateOne)
    givenEditProjectAPICall(slug, accessToken, returning = error.asLeft.pure[IO])

    finder.updateProject(slug, newValues, accessToken).asserting(_.left.value shouldBe error)
  }

  it should "succeed and return updated values if PUT gl/projects/:slug returns 200 OK" in {
    val updatedProject = glUpdatedProjectsGen.generateOne
    mapResponse(Ok, Request[IO](), Response[IO]().withEntity(updatedProject.asJson))
      .asserting(_.value shouldBe updatedProject)
  }

  it should "return left if PUT gl/projects/:slug returns 400 BAD_REQUEST with an error" in {

    val error = nonEmptyStrings().generateOne

    mapResponse(BadRequest, Request[IO](), Response[IO](BadRequest).withEntity(json"""{"error": $error}"""))
      .asserting(
        _.left.value shouldBe UpdateFailures.badRequestOnGLUpdate(Message.Error.fromJsonUnsafe(Json.fromString(error)))
      )
  }

  it should "return left if PUT gl/projects/:slug returns 400 BAD_REQUEST with a message" in {

    val message = jsons.generateOne

    mapResponse(BadRequest, Request[IO](), Response[IO](BadRequest).withEntity(json"""{"message": $message}"""))
      .asserting(_.left.value shouldBe UpdateFailures.badRequestOnGLUpdate(Message.Error.fromJsonUnsafe(message)))
  }

  it should "return left if PUT gl/projects/:slug returns 403 FORBIDDEN with a message" in {

    val message = jsons.generateOne

    mapResponse(Forbidden, Request[IO](), Response[IO](Forbidden).withEntity(json"""{"message": $message}"""))
      .asserting(_.left.value shouldBe UpdateFailures.forbiddenOnGLUpdate(Message.Error.fromJsonUnsafe(message)))
  }

  private implicit val glClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val finder = new GLProjectUpdaterImpl[IO]

  private def givenEditProjectAPICall(slug:        projects.Slug,
                                      accessToken: AccessToken,
                                      returning:   IO[Either[Message, GLUpdatedProject]]
  ) = {
    val multipartCaptor = CaptureOne[Multipart[IO]]()
    val endpointName: String Refined NonEmpty = "edit-project"
    (glClient
      .put(_: Uri, _: String Refined NonEmpty, _: Multipart[IO])(
        _: ResponseMappingF[IO, Either[Message, GLUpdatedProject]]
      )(_: Option[AccessToken]))
      .expects(uri"projects" / slug, endpointName, capture(multipartCaptor), *, accessToken.some)
      .returning(returning)
    multipartCaptor
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
      underlyingMethod = Put
    )

  private def verifyRequest(multipartCaptor: CaptureOne[Multipart[IO]], newValues: ProjectUpdates) = {
    import io.renku.knowledgegraph.multipart.syntax._
    import io.renku.knowledgegraph.projects.images.Image
    import io.renku.knowledgegraph.projects.images.MultipartImageCodecs.imagePartDecoder

    val parts = multipartCaptor.value.parts

    def findPart(name: String) =
      parts
        .find(_.name.value == name)
        .getOrElse(fail(s"No '$name' part"))

    val visCheck = newValues.newVisibility
      .map(v => findPart("visibility").as[projects.Visibility].asserting(_ shouldBe v))
      .getOrElse(Succeeded.pure[IO])

    val imageCheck = newValues.newImage
      .map(v => findPart("avatar").as[Option[Image]].asserting(_ shouldBe v))
      .getOrElse(Succeeded.pure[IO])

    visCheck >> imageCheck
  }

  private implicit lazy val responseEncoder: Encoder[GLUpdatedProject] = Encoder.instance {
    case GLUpdatedProject(image, visibility) =>
      json"""{
        "avatar_url": $image,
        "visibility": $visibility
      }"""
  }
}
