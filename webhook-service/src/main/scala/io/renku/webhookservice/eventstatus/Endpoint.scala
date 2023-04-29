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

package io.renku.webhookservice
package eventstatus

import cats.MonadThrow
import cats.data.EitherT
import cats.effect._
import cats.syntax.all._
import io.circe.syntax._
import io.renku.graph.eventlog
import io.renku.graph.eventlog.api.events.CommitSyncRequest
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.http.ErrorMessage._
import io.renku.http.client.GitLabClient
import io.renku.http.server.security.model.AuthUser
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.logging.ExecutionTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.webhookservice.hookvalidation
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.{HookValidationResult, NoAccessTokenException}
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.Response
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def fetchProcessingStatus(projectId: GitLabId, authUser: Option[AuthUser]): F[Response[F]]
}

private class EndpointImpl[F[_]: MonadThrow: Logger: ExecutionTimeRecorder](
    hookValidator:     HookValidator[F],
    statusInfoFinder:  StatusInfoFinder[F],
    projectInfoFinder: ProjectInfoFinder[F],
    elClient:          eventlog.api.events.Client[F]
) extends Http4sDsl[F]
    with Endpoint[F] {

  import HookValidationResult._
  import projectInfoFinder.findProjectInfo
  import statusInfoFinder.findStatusInfo
  private val executionTimeRecorder = ExecutionTimeRecorder[F]
  import executionTimeRecorder._

  def fetchProcessingStatus(projectId: GitLabId, authUser: Option[AuthUser]): F[Response[F]] = measureExecutionTime {
    validateHook(projectId, authUser)
      .semiflatMap {
        case HookMissing =>
          Ok(StatusInfo.NotActivated.asJson)
        case HookExists =>
          findStatusInfo(projectId)
            .flatTap(sendCommitSyncIfNone(projectId, authUser))
            .map(_.getOrElse(StatusInfo.webhookReady.widen))
            .flatMap(si => Ok(si.asJson))
      }
      .merge
      .recoverWith(internalServerError(projectId))
  } map logExecutionTime(withMessage = show"Finding status info for project '$projectId' finished")

  private def validateHook(projectId: GitLabId, authUser: Option[AuthUser]) =
    EitherT {
      hookValidator
        .validateHook(projectId, authUser.map(_.accessToken))
        .map(_.asRight[Response[F]])
        .recoverWith(noAccessTokenToNotFound)
    }

  private lazy val noAccessTokenToNotFound: PartialFunction[Throwable, F[Either[Response[F], HookValidationResult]]] = {
    case _: NoAccessTokenException => NotFound(InfoMessage("Info about project cannot be found")).map(_.asLeft)
  }

  private def sendCommitSyncIfNone(projectId: GitLabId, authUser: Option[AuthUser]): Option[StatusInfo] => F[Unit] = {
    case Some(_) => ().pure[F]
    case None =>
      findProjectInfo(projectId)(authUser.map(_.accessToken)).map(CommitSyncRequest(_)) >>=
        elClient.send
  }

  private def internalServerError(projectId: projects.GitLabId): PartialFunction[Throwable, F[Response[F]]] = {
    case exception =>
      val message = show"Finding status info for project '$projectId' failed"
      Logger[F].error(exception)(message) >> InternalServerError(ErrorMessage(message))
  }
}

object Endpoint {
  def apply[F[_]: Async: GitLabClient: ExecutionTimeRecorder: Logger: MetricsRegistry](
      projectHookUrl: ProjectHookUrl
  ): F[Endpoint[F]] = for {
    hookValidator     <- hookvalidation.HookValidator(projectHookUrl)
    statusInfoFinder  <- StatusInfoFinder[F]
    projectInfoFinder <- ProjectInfoFinder[F]
    elClient          <- eventlog.api.events.Client[F]
  } yield new EndpointImpl[F](hookValidator, statusInfoFinder, projectInfoFinder, elClient)
}
