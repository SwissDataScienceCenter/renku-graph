/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository.association

import cats.MonadError
import cats.effect.Async
import cats.effect.kernel.{Concurrent, Temporal}
import cats.syntax.all._
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.graph.model.projects.Id
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.http.client.AccessToken
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.ProjectsTokensDB
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.{EntityDecoder, Request, Response}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait AssociateTokenEndpoint[Interpretation[_]] {
  def associateToken(projectId: Id, request: Request[Interpretation]): Interpretation[Response[Interpretation]]
}

class AssociateTokenEndpointImpl[Interpretation[_]: Concurrent: Logger](
    tokenAssociator: TokenAssociator[Interpretation]
) extends Http4sDsl[Interpretation]
    with AssociateTokenEndpoint[Interpretation] {

  import tokenAssociator._

  override def associateToken(projectId: Id,
                              request:   Request[Interpretation]
  ): Interpretation[Response[Interpretation]] = {
    for {
      accessToken <- request.as[AccessToken] recoverWith badRequest
      _           <- associate(projectId, accessToken)
      response    <- NoContent()
    } yield response
  } recoverWith httpResponse(projectId)

  private implicit lazy val accessTokenEntityDecoder: EntityDecoder[Interpretation, AccessToken] =
    jsonOf[Interpretation, AccessToken]

  private case class BadRequestError(cause: Throwable) extends Exception(cause)

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[AccessToken]] = { case NonFatal(exception) =>
    MonadError[Interpretation, Throwable].raiseError(BadRequestError(exception))
  }

  private def httpResponse(projectId: Id): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case BadRequestError(exception) =>
      BadRequest(ErrorMessage(exception))
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage(s"Associating token with projectId: $projectId failed")
      Logger[Interpretation].error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }
}

object AssociateTokenEndpoint {

  def apply[F[_]: Async: Temporal: Logger](
      sessionResource:  SessionResource[F, ProjectsTokensDB],
      queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[AssociateTokenEndpoint[F]] = for {
    tokenAssociator <- TokenAssociator(sessionResource, queriesExecTimes)
  } yield new AssociateTokenEndpointImpl[F](tokenAssociator)
}
