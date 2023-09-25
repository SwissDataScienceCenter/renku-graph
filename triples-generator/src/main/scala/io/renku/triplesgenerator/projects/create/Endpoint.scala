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

package io.renku.triplesgenerator.projects.create

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.triplesgenerator.TgLockDB.TsWriteLock
import io.renku.triplesgenerator.api.NewProject
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def `POST /projects`(request: Request[F]): F[Response[F]]
}

object Endpoint {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](tsWriteLock: TsWriteLock[F]): F[Endpoint[F]] =
    ProjectCreator[F](tsWriteLock).map(new EndpointImpl[F](_))
}

private class EndpointImpl[F[_]: Async: Logger](projectCreator: ProjectCreator[F])
    extends Http4sDsl[F]
    with Endpoint[F] {

  override def `POST /projects`(request: Request[F]): F[Response[F]] =
    EitherT(decodePayload(request))
      .semiflatTap(projectCreator.createProject)
      .semiflatTap(project => Logger[F].info(show"""$reportingPrefix: project $project created"""))
      .as(Response[F](Created).withEntity(Message.Info("Project created")))
      .merge
      .handleErrorWith(errorHttpResult)

  private def decodePayload: Request[F] => F[Either[Response[F], NewProject]] =
    _.as[NewProject].map(_.asRight[Response[F]]).handleError(badRequest)

  private lazy val badRequest: Throwable => Either[Response[F], NewProject] = { _ =>
    Response[F](BadRequest).withEntity(Message.Error("Invalid payload")).asLeft[NewProject]
  }

  private lazy val errorHttpResult: Throwable => F[Response[F]] = ex =>
    Logger[F].error(ex)(show"$reportingPrefix: project creation failed") >>
      InternalServerError(Message.Error.fromMessageAndStackTraceUnsafe("Project creation failed", ex))
}
