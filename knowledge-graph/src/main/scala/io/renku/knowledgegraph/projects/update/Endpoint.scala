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

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.circe.syntax._
import io.renku.data.ErrorMessage
import io.renku.graph.model.projects
import io.renku.http.InfoMessage
import io.renku.http.InfoMessage._
import io.renku.http.client.GitLabClient
import io.renku.http.server.security.model.AuthUser
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.SyncRepoMetadata
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response}
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def `PUT /projects/:path`(path: projects.Path, request: Request[F], authUser: AuthUser): F[Response[F]]
}

object Endpoint {
  def apply[F[_]: Async: Logger: MetricsRegistry: GitLabClient]: F[Endpoint[F]] =
    triplesgenerator.api.events.Client[F].map(new EndpointImpl(GLProjectUpdater[F], _))
}

private class EndpointImpl[F[_]: Async: Logger](glProjectUpdater: GLProjectUpdater[F],
                                                tgClient: triplesgenerator.api.events.Client[F]
) extends Http4sDsl[F]
    with Endpoint[F] {

  import glProjectUpdater.updateProject
  import tgClient.send

  override def `PUT /projects/:path`(path: projects.Path, request: Request[F], authUser: AuthUser): F[Response[F]] =
    decodePayload(request)
      .flatMap(updateGLAndSendEvent(path, authUser))
      .merge

  private lazy val decodePayload: Request[F] => EitherT[F, Response[F], NewValues] = req =>
    EitherT {
      req
        .as[NewValues]
        .map(_.asRight[Response[F]])
        .handleError(badRequest(_).asLeft[NewValues])
    }

  private lazy val badRequest: Throwable => Response[F] = { _ =>
    Response[F](BadRequest).withEntity(ErrorMessage("Invalid payload").asJson)
  }

  private def updateGLAndSendEvent(path: projects.Path, authUser: AuthUser)(
      newValues: NewValues
  ): EitherT[F, Response[F], Response[F]] =
    EitherT {
      (updateProject(path, newValues, authUser.accessToken) >> send(SyncRepoMetadata(path, None)))
        .as(Response[F](Accepted).withEntity(InfoMessage("Project update accepted")).asRight[Response[F]])
        .handleErrorWith(serverError(path)(_).map(_.asLeft[Response[F]]))
    }

  private def serverError(path: projects.Path): Throwable => F[Response[F]] =
    Logger[F]
      .error(_)(show"Updating project $path failed")
      .as(Response[F](InternalServerError).withEntity(ErrorMessage("Update failed").asJson))
}
