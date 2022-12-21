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

package io.renku.webhookservice.eventstatus

import cats.MonadThrow
import cats.data.EitherT
import cats.effect._
import cats.syntax.all._
import io.circe.syntax._
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.http.ErrorMessage._
import io.renku.http.client.GitLabClient
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.logging.ExecutionTimeRecorder
import io.renku.webhookservice.hookvalidation
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.{HookValidationResult, NoAccessTokenException}
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.Response
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def fetchProcessingStatus(projectId: GitLabId): F[Response[F]]
}

private class EndpointImpl[F[_]: MonadThrow: Logger: ExecutionTimeRecorder](
    hookValidator:    HookValidator[F],
    statusInfoFinder: StatusInfoFinder[F]
) extends Http4sDsl[F]
    with Endpoint[F] {

  import HookValidationResult._
  private val executionTimeRecorder = ExecutionTimeRecorder[F]
  import executionTimeRecorder._

  def fetchProcessingStatus(projectId: GitLabId): F[Response[F]] = measureExecutionTime {
    validateHook(projectId)
      .semiflatMap {
        case HookExists  => findStatus(projectId)
        case HookMissing => Ok(StatusInfo.NotActivated.asJson)
      }
      .merge
      .recoverWith(internalServerError(projectId))
  } map logExecutionTime(withMessage = show"Finding status info for project '$projectId' finished")

  private def validateHook(projectId: GitLabId): EitherT[F, Response[F], HookValidationResult] = EitherT {
    hookValidator
      .validateHook(projectId, maybeAccessToken = None)
      .map(_.asRight[Response[F]])
      .recoverWith(noAccessTokenToNotFound)
  }

  private lazy val noAccessTokenToNotFound: PartialFunction[Throwable, F[Either[Response[F], HookValidationResult]]] = {
    case _: NoAccessTokenException => NotFound(InfoMessage("Info about project cannot be found")).map(_.asLeft)
  }

  private def findStatus(projectId: GitLabId) =
    statusInfoFinder
      .findStatusInfo(projectId)
      .biSemiflatMap(internalServerError(projectId), status => Ok(status.asJson))
      .merge

  private def internalServerError(projectId: projects.GitLabId): PartialFunction[Throwable, F[Response[F]]] = {
    case exception =>
      val message = show"Finding status info for project '$projectId' failed"
      Logger[F].error(exception)(message) >> InternalServerError(ErrorMessage(message))
  }
}

object Endpoint {
  def apply[F[_]: Async: GitLabClient: ExecutionTimeRecorder: Logger](
      projectHookUrl: ProjectHookUrl
  ): F[Endpoint[F]] = for {
    finder        <- StatusInfoFinder[F]
    hookValidator <- hookvalidation.HookValidator(projectHookUrl)
  } yield new EndpointImpl[F](hookValidator, finder)
}
