/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.NonEmptyParallel
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import eu.timepit.refined.auto._
import io.circe.literal._
import io.renku.data.Message
import io.renku.graph.model.projects
import io.renku.http.client.GitLabClient
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.Failure
import io.renku.metrics.MetricsRegistry
import org.http4s.Status.{BadRequest, Conflict, Created, Forbidden, InternalServerError}
import org.http4s.multipart.Multipart
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger
import org.http4s.circe.CirceEntityCodec._

trait Endpoint[F[_]] {
  def `POST /projects`(request: Request[F], authUser: AuthUser): F[Response[F]]
}

object Endpoint {
  def apply[F[_]: Async: NonEmptyParallel: Logger: MetricsRegistry: GitLabClient](config: Config): F[Endpoint[F]] =
    ProjectCreator[F](config).map(new EndpointImpl[F](_))
}

private class EndpointImpl[F[_]: Async: Logger](projectCreator: ProjectCreator[F]) extends Endpoint[F] {

  override def `POST /projects`(request: Request[F], authUser: AuthUser): F[Response[F]] =
    EitherT(decodePayload(request))
      .semiflatMap { newProject =>
        projectCreator
          .createProject(newProject, authUser)
          .fproduct(toAccepted)
          .flatTap { case (slug, _) => Logger[F].info(show"Project $slug created") }
          .map { case (_, response) => response }
      }
      .merge
      .handleErrorWith(relevantError)

  private lazy val decodePayload: Request[F] => F[Either[Response[F], NewProject]] =
    _.as[Multipart[F]]
      .flatMap(MultipartRequestDecoder[F].decode)
      .map(_.asRight[Response[F]])
      .handleError(badRequest(_).asLeft[NewProject])

  private def badRequest: Throwable => Response[F] = { _ =>
    Response[F](BadRequest).withEntity(Message.Error("Invalid payload"))
  }

  private def toAccepted(slug: projects.Slug): Response[F] =
    Response[F](Created).withEntity {
      Message.Info.fromJsonUnsafe {
        json"""{
          "message": "Project created",
          "slug":    $slug
        }"""
      }
    }

  private lazy val relevantError: Throwable => F[Response[F]] = {
    case f: Failure =>
      logFailure(f).as(f.toResponse[F])
    case ex: Exception =>
      Logger[F]
        .error(ex)("Creating project failed")
        .as(Response[F](InternalServerError).withEntity(Message.Error("Creation failed")))
  }

  private lazy val logFailure: Failure => F[Unit] = {
    case f if f.status == BadRequest || f.status == Conflict =>
      Logger[F].info(show"Creating project failed: ${f.detailedMessage}")
    case f if f.status == Forbidden =>
      ().pure[F]
    case f =>
      Logger[F].error(f)(show"Creating project failed: ${f.detailedMessage}")
  }
}
