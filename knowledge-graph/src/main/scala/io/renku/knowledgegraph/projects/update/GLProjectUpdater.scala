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

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.{Decoder, Json}
import io.renku.data.Message
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.knowledgegraph.Failure
import io.renku.knowledgegraph.multipart.syntax._
import io.renku.knowledgegraph.projects.images.Image
import io.renku.knowledgegraph.projects.images.MultipartImageCodecs.imagePartEncoder
import org.http4s.Status._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.jsonOf
import org.http4s.headers.{`Content-Disposition`, `Content-Type`}
import org.http4s.implicits._
import org.http4s.multipart.{Multipart, Multiparts, Part}
import org.http4s.{Headers, MediaType, Request, Response, Status}
import org.typelevel.ci._

private trait GLProjectUpdater[F[_]] {
  def updateProject(slug:    projects.Slug,
                    updates: ProjectUpdates,
                    at:      AccessToken
  ): F[Either[Failure, Option[GLUpdatedProject]]]
}

private object GLProjectUpdater {
  def apply[F[_]: Async: GitLabClient]: GLProjectUpdater[F] = new GLProjectUpdaterImpl[F]
}

private class GLProjectUpdaterImpl[F[_]: Async: GitLabClient] extends GLProjectUpdater[F] {

  override def updateProject(slug:    projects.Slug,
                             updates: ProjectUpdates,
                             at:      AccessToken
  ): F[Either[Failure, Option[GLUpdatedProject]]] =
    if (updates.glUpdateNeeded) {
      implicit val token: Option[AccessToken] = at.some
      toMultipart(updates).flatMap(
        GitLabClient[F]
          .put(uri"projects" / slug, "edit-project", _)(mapResponse)
          .map(_.map(_.some))
      )
    } else
      Option.empty[GLUpdatedProject].asRight[Failure].pure[F]

  private lazy val toMultipart: ProjectUpdates => F[Multipart[F]] = {
    case ProjectUpdates(_, newImage, _, newVisibility) =>
      Multiparts
        .forSync[F]
        .flatMap(_.multipart(Vector(maybeVisibilityPart(newVisibility), maybeAvatarPart(newImage)).flatten))
  }

  private def maybeVisibilityPart(newVisibility: Option[projects.Visibility]) =
    newVisibility.map(_.asPart[F]("visibility"))

  private def maybeAvatarPart(newImage: Option[Option[Image]]) =
    newImage.map(
      _.fold(
        Part[F](
          Headers(`Content-Disposition`("form-data", Map(ci"name" -> "avatar")), `Content-Type`(MediaType.image.jpeg)),
          fs2.Stream.empty
        )
      )(_.asPart[F]("avatar"))
    )

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[Either[Failure, GLUpdatedProject]]] = {
    case (Ok, _, resp) =>
      resp.as[GLUpdatedProject].map(_.asRight[Failure])
    case (BadRequest, _, resp) =>
      resp
        .as[Json](MonadThrow[F], jsonOf(Async[F], errorDecoder))
        .map(Message.Error.fromJsonUnsafe)
        .map(UpdateFailures.badRequestOnGLUpdate)
        .map(_.asLeft)
    case (Forbidden, _, resp) =>
      resp
        .as[Json](MonadThrow[F], jsonOf(Async[F], errorDecoder))
        .map(Message.Error.fromJsonUnsafe)
        .map(UpdateFailures.forbiddenOnGLUpdate)
        .map(_.asLeft)
  }

  private lazy val errorDecoder: Decoder[Json] = { cur =>
    (cur.downField("error").as[Option[Json]], cur.downField("message").as[Option[Json]])
      .mapN(_ orElse _ getOrElse cur.value)
  }
}
