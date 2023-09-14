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

package io.renku.triplesgenerator.projects.update

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.graph.model.projects
import io.renku.triplesgenerator.TgLockDB.TsWriteLock
import io.renku.triplesgenerator.api.ProjectUpdates
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def `PATCH /projects/:slug`(slug: projects.Slug, request: Request[F]): F[Response[F]]
}

object Endpoint {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](tsWriteLock: TsWriteLock[F]): F[Endpoint[F]] = for {
    projectUpdater <- ProjectUpdater[F](tsWriteLock)
  } yield new EndpointImpl[F](projectUpdater)
}

private class EndpointImpl[F[_]: Async: Logger](projectUpdater: ProjectUpdater[F])
    extends Http4sDsl[F]
    with Endpoint[F] {

  override def `PATCH /projects/:slug`(slug: projects.Slug, request: Request[F]): F[Response[F]] =
    EitherT(decodePayload(request))
      .semiflatMap(updates => projectUpdater.updateProject(slug, updates).map(updates -> _))
      .semiflatMap { case (updates, result) =>
        Logger[F].info(show"""project $slug updated with $updates""").as(result)
      }
      .map(toHttpResult)
      .merge
      .handleErrorWith(errorHttpResult(slug))

  private def decodePayload: Request[F] => F[Either[Response[F], ProjectUpdates]] =
    _.as[ProjectUpdates].map(_.asRight[Response[F]]).handleError(badRequest)

  private lazy val badRequest: Throwable => Either[Response[F], ProjectUpdates] = { _ =>
    Response[F](BadRequest).withEntity(Message.Error("Invalid payload")).asLeft[ProjectUpdates]
  }

  private lazy val toHttpResult: ProjectUpdater.Result => Response[F] = {
    case ProjectUpdater.Result.Updated   => Response[F](Ok).withEntity(Message.Info("Project updated"))
    case ProjectUpdater.Result.NotExists => Response[F](NotFound).withEntity(Message.Info("Project not found"))
  }

  private def errorHttpResult(slug: projects.Slug): Throwable => F[Response[F]] = ex =>
    Logger[F].error(ex)(s"Project $slug update failed") >>
      InternalServerError(Message.Error.fromMessageAndStackTraceUnsafe("Project update failed", ex))
}
