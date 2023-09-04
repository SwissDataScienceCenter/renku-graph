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

import cats.NonEmptyParallel
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.graph.model.projects
import io.renku.http.client.GitLabClient
import io.renku.http.server.security.model.AuthUser
import io.renku.metrics.MetricsRegistry
import org.http4s.MediaType.{application, multipartType}
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.http4s.multipart.Multipart
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def `PATCH /projects/:slug`(slug: projects.Slug, request: Request[F], authUser: AuthUser): F[Response[F]]
}

object Endpoint {
  def apply[F[_]: Async: NonEmptyParallel: Logger: MetricsRegistry: GitLabClient]: F[Endpoint[F]] =
    ProjectUpdater[F].map(new EndpointImpl(_))
}

private class EndpointImpl[F[_]: Async: Logger](projectUpdater: ProjectUpdater[F])
    extends Http4sDsl[F]
    with Endpoint[F] {

  override def `PATCH /projects/:slug`(slug: projects.Slug, request: Request[F], authUser: AuthUser): F[Response[F]] =
    EitherT(decodePayload(request))
      .semiflatMap { updates =>
        projectUpdater
          .updateProject(slug, updates, authUser)
          .as(Response[F](Accepted).withEntity(Message.Info("Project update accepted")))
          .flatTap(_ => Logger[F].info(show"Project $slug updated with $updates"))
      }
      .merge
      .handleErrorWith(relevantError(slug))

  private lazy val decodePayload: Request[F] => F[Either[Response[F], ProjectUpdates]] = {
    case req if req.contentType contains `Content-Type`(application.json) =>
      req
        .as[ProjectUpdates]
        .map(_.asRight[Response[F]])
        .handleError(badRequest(_).asLeft[ProjectUpdates])
    case req if req.contentType.map(_.mediaType).exists(_.satisfies(multipartType("form-data"))) =>
      req
        .as[Multipart[F]]
        .flatMap(MultipartRequestDecoder[F].decode)
        .map(_.asRight[Response[F]])
        .handleError(badRequest(_).asLeft[ProjectUpdates])
    case req =>
      Response[F](InternalServerError)
        .withEntity(Message.Error.unsafeApply(s"'${req.contentType}' not supported"))
        .asLeft[ProjectUpdates]
        .pure[F]
  }

  private def badRequest: Throwable => Response[F] = { _ =>
    Response[F](BadRequest).withEntity(Message.Error("Invalid payload"))
  }

  private def relevantError(slug: projects.Slug): Throwable => F[Response[F]] = {
    case f: Failure =>
      logFailure(slug)(f)
        .as(Response[F](f.status).withEntity(f.message))
    case ex: Exception =>
      Logger[F]
        .error(ex)(show"Updating project $slug failed")
        .as(Response[F](InternalServerError).withEntity(Message.Error("Update failed")))
  }

  private def logFailure(slug: projects.Slug): Failure => F[Unit] = {
    case f if f.status == BadRequest || f.status == Conflict =>
      Logger[F].info(show"Updating project $slug failed: ${f.getMessage}")
    case f if f.status == Forbidden =>
      ().pure[F]
    case f =>
      Logger[F].error(f)(show"Updating project $slug failed")
  }
}
