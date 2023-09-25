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

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.{Decoder, Json}
import io.renku.data.Message
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.knowledgegraph.Failure
import io.renku.knowledgegraph.multipart.syntax._
import io.renku.knowledgegraph.projects.images.MultipartImageCodecs.imagePartEncoder
import org.http4s.Status.{BadRequest, Created, Forbidden}
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.jsonOf
import org.http4s.implicits._
import org.http4s.multipart.{Multipart, Multiparts}
import org.http4s.{Request, Response, Status}

private trait GLProjectCreator[F[_]] {
  def createProject(newProject: NewProject, accessToken: AccessToken): F[Either[Failure, GLCreatedProject]]
}

private object GLProjectCreator {
  def apply[F[_]: Async: GitLabClient]: GLProjectCreator[F] = new GLProjectCreatorImpl[F]
}

private class GLProjectCreatorImpl[F[_]: Async: GitLabClient] extends GLProjectCreator[F] {

  override def createProject(newProject: NewProject, accessToken: AccessToken): F[Either[Failure, GLCreatedProject]] = {
    implicit val at: AccessToken = accessToken

    toMultipart(newProject).flatMap(
      GitLabClient[F]
        .postMultipart(uri"projects", "create-project", _)(mapResponse)
    )
  }

  private lazy val toMultipart: NewProject => F[Multipart[F]] = { newProject =>
    val parts =
      newProject.maybeImage
        .asParts[F]("avatar")
        .toVector
        .appended(newProject.name.asPart[F]("name"))
        .appended(newProject.namespace.identifier.asPart[F]("namespace_id"))
        .appended(newProject.visibility.asPart[F]("visibility"))
        .appended(newProject.keywords.toList.mkString_(",").asPart[F]("topics"))
        .appended(newProject.branch.asPart[F]("default_branch"))

    Multiparts.forSync[F].flatMap(_.multipart(parts))
  }

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[Either[Failure, GLCreatedProject]]] = {
    case (Created, _, resp) =>
      resp.as[GLCreatedProject].map(_.asRight[Failure])
    case (BadRequest, _, resp) =>
      resp
        .as[Json](MonadThrow[F], jsonOf(Async[F], errorDecoder))
        .map(Message.Error.fromJsonUnsafe)
        .map(CreationFailures.badRequestOnGLCreate)
        .map(_.asLeft)
    case (Forbidden, _, resp) =>
      resp
        .as[Json](MonadThrow[F], jsonOf(Async[F], errorDecoder))
        .map(Message.Error.fromJsonUnsafe)
        .map(CreationFailures.forbiddenOnGLCreate)
        .map(_.asLeft)
  }

  private lazy val errorDecoder: Decoder[Json] = { cur =>
    (cur.downField("error").as[Option[Json]], cur.downField("message").as[Option[Json]])
      .mapN(_ orElse _ getOrElse cur.value)
  }
}
