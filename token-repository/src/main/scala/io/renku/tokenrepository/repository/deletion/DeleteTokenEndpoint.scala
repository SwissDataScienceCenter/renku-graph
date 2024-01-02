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

package io.renku.tokenrepository.repository.deletion

import cats.effect.Async
import cats.syntax.all._
import io.renku.data.Message
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import org.http4s.Response
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait DeleteTokenEndpoint[F[_]] {
  def deleteToken(projectId: GitLabId, maybeAccessToken: Option[AccessToken]): F[Response[F]]
}

class DeleteTokenEndpointImpl[F[_]: Async: Logger](tokenRemover: TokenRemover[F])
    extends Http4sDsl[F]
    with DeleteTokenEndpoint[F] {

  override def deleteToken(projectId: GitLabId, maybeAccessToken: Option[AccessToken]): F[Response[F]] =
    (tokenRemover.delete(projectId, maybeAccessToken).flatMap(logSuccess(projectId)) >> NoContent())
      .recoverWith(errorHttpResult(projectId))

  private def logSuccess(projectId: GitLabId): DeletionResult => F[Unit] = {
    case DeletionResult.Deleted    => Logger[F].info(show"token removed for $projectId")
    case DeletionResult.NotExisted => ().pure[F]
  }

  private def errorHttpResult(projectId: GitLabId): PartialFunction[Throwable, F[Response[F]]] = {
    case NonFatal(exception) =>
      val message = Message.Error.unsafeApply(s"Deleting token for projectId: $projectId failed")
      Logger[F].error(exception)(message.show) >> InternalServerError(message)
  }
}

object DeleteTokenEndpoint {
  def apply[F[_]: Async: GitLabClient: Logger: SessionResource: QueriesExecutionTimes]: F[DeleteTokenEndpoint[F]] =
    TokenRemover[F].map(new DeleteTokenEndpointImpl[F](_))
}
