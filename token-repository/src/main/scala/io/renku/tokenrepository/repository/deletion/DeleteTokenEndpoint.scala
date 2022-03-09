/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository.deletion

import cats.MonadThrow
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.SessionResource
import io.renku.graph.model.projects.Id
import io.renku.http.ErrorMessage
import io.renku.http.ErrorMessage._
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.ProjectsTokensDB
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait DeleteTokenEndpoint[F[_]] {
  def deleteToken(projectId: Id): F[Response[F]]
}

class DeleteTokenEndpointImpl[F[_]: MonadThrow: Logger](
    tokenRemover: TokenRemover[F]
) extends Http4sDsl[F]
    with DeleteTokenEndpoint[F] {

  override def deleteToken(projectId: Id): F[Response[F]] =
    tokenRemover
      .delete(projectId)
      .flatMap(_ => NoContent())
      .recoverWith(httpResult(projectId))

  private def httpResult(projectId: Id): PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage(s"Deleting token for projectId: $projectId failed")
    Logger[F].error(exception)(errorMessage.value)
    InternalServerError(errorMessage)
  }
}

object DeleteTokenEndpoint {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource:  SessionResource[F, ProjectsTokensDB],
      queriesExecTimes: LabeledHistogram[F]
  ): F[DeleteTokenEndpoint[F]] = MonadThrow[F].catchNonFatal {
    new DeleteTokenEndpointImpl[F](new TokenRemoverImpl[F](sessionResource, queriesExecTimes))
  }
}
