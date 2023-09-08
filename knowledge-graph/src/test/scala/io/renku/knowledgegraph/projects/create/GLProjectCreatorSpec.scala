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

package io.renku.knowledgegraph.projects.create

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
import io.renku.core.client.Branch
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{jsons, nonEmptyStrings}
import io.renku.graph.model.projects
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.testtools.GitLabClientTools
import org.http4s.Status.{BadRequest, Created, Forbidden}
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.implicits._
import org.http4s.multipart.Multipart
import org.http4s.{Request, Response, Uri}
import org.scalamock.matchers.ArgCapture.CaptureOne
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatest.{EitherValues, OptionValues, Succeeded}

class GLProjectCreatorSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with AsyncMockFactory
    with should.Matchers
    with EitherValues
    with OptionValues
    with GitLabClientTools[IO] {

  it should s"call GL's POST gl/projects and return created values on success" in {

    val accessToken    = accessTokens.generateOne
    val createdProject = glCreatedProjectsGen.generateOne

    val multipartCaptor = givenCreateProjectAPICall(accessToken, returning = createdProject.asRight.pure[IO])

    val newProject = newProjects.generateOne

    creator
      .createProject(newProject, accessToken)
      .asserting(_.value shouldBe createdProject)
      .flatMap(_ => verifyRequest(multipartCaptor, newProject))
  }

  it should s"call GL's POST gl/projects and return GL message if returned" in {

    val accessToken = accessTokens.generateOne

    val error = Message.Error.fromJsonUnsafe(jsons.generateOne)
    givenCreateProjectAPICall(accessToken, returning = error.asLeft.pure[IO])

    val newProject = newProjects.generateOne

    creator.createProject(newProject, accessToken).asserting(_.left.value shouldBe error)
  }

  it should "succeed and return created values if POST gl/projects returns 201 Created" in {
    val createdProject = glCreatedProjectsGen.generateOne
    mapResponse(Created, Request[IO](), Response[IO]().withEntity(createdProject.asJson))
      .asserting(_.value shouldBe createdProject)
  }

  it should "return left if POST gl/projects returns 400 BAD_REQUEST with an error" in {

    val error = nonEmptyStrings().generateOne

    mapResponse(BadRequest, Request[IO](), Response[IO](BadRequest).withEntity(json"""{"error": $error}"""))
      .asserting(
        _.left.value shouldBe UpdateFailures.badRequestOnGLCreate(Message.Error.fromJsonUnsafe(Json.fromString(error)))
      )
  }

  it should "return left if POST gl/projects returns 400 BAD_REQUEST with a message" in {

    val message = jsons.generateOne

    mapResponse(BadRequest, Request[IO](), Response[IO](BadRequest).withEntity(json"""{"message": $message}"""))
      .asserting(_.left.value shouldBe UpdateFailures.badRequestOnGLCreate(Message.Error.fromJsonUnsafe(message)))
  }

  it should "return left if POST gl/projects returns 403 FORBIDDEN with a message" in {

    val message = jsons.generateOne

    mapResponse(Forbidden, Request[IO](), Response[IO](Forbidden).withEntity(json"""{"message": $message}"""))
      .asserting(_.left.value shouldBe UpdateFailures.forbiddenOnGLCreate(Message.Error.fromJsonUnsafe(message)))
  }

  private implicit val glClient: GitLabClient[IO] = mock[GitLabClient[IO]]
  private lazy val creator = new GLProjectCreatorImpl[IO]

  private def givenCreateProjectAPICall(accessToken: AccessToken, returning: IO[Either[Message, GLCreatedProject]]) = {
    val multipartCaptor = CaptureOne[Multipart[IO]]()
    val endpointName: String Refined NonEmpty = "create-project"
    (glClient
      .postMultipart(_: Uri, _: String Refined NonEmpty, _: Multipart[IO])(
        _: ResponseMappingF[IO, Either[Message, GLCreatedProject]]
      )(_: AccessToken))
      .expects(uri"projects", endpointName, capture(multipartCaptor), *, accessToken)
      .returning(returning)
    multipartCaptor
  }

  private lazy val mapResponse: ResponseMappingF[IO, Either[Message, GLCreatedProject]] =
    captureMapping(glClient)(
      creator
        .createProject(newProjects.generateOne, accessTokens.generateOne)
        .unsafeRunSync(),
      glCreatedProjectsGen.generateOne.asRight[Message],
      underlyingMethod = PostMultipart
    )

  private def verifyRequest(multipartCaptor: CaptureOne[Multipart[IO]], newProject: NewProject) = {
    import io.renku.knowledgegraph.multipart.syntax._
    import io.renku.knowledgegraph.projects.images.Image
    import io.renku.knowledgegraph.projects.images.MultipartImageCodecs.imagePartDecoder

    val parts = multipartCaptor.value.parts

    def findPart(name: String) =
      parts
        .find(_.name.value == name)
        .getOrElse(fail(s"No '$name' part"))

    findPart("name").as[projects.Name].asserting(_ shouldBe newProject.name) >>
      findPart("path").as[String].asserting(_ shouldBe newProject.slug.value.split("/").last) >>
      findPart("namespace_id").as[NamespaceId].asserting(_ shouldBe newProject.namespaceId) >>
      findPart("visibility").as[projects.Visibility].asserting(_ shouldBe newProject.visibility) >>
      findPart("default_branch").as[Branch].asserting(_ shouldBe newProject.branch) >>
      newProject.maybeImage
        .map(v => findPart("avatar").as[Option[Image]].asserting(_ shouldBe v.some))
        .getOrElse(Succeeded.pure[IO])
  }

  private implicit lazy val responseEncoder: Encoder[GLCreatedProject] = Encoder.instance {
    case GLCreatedProject(image) =>
      json"""{
        "avatar_url": $image
      }"""
  }
}
