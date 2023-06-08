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

import cats.effect._
import cats.syntax.all._
import cats.{MonadThrow, NonEmptyParallel}
import io.circe.syntax._
import io.renku.eventlog.api.events.CommitSyncRequest
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.http.ErrorMessage._
import io.renku.http.client.GitLabClient
import io.renku.http.server.security.model.AuthUser
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.logging.ExecutionTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import io.renku.webhookservice.hookvalidation
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult
import io.renku.webhookservice.model.ProjectHookUrl
import io.renku.{eventlog, triplesgenerator}
import org.http4s.Response
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def fetchProcessingStatus(projectId: GitLabId, authUser: Option[AuthUser]): F[Response[F]]
}

private class EndpointImpl[F[_]: MonadThrow: NonEmptyParallel: Logger: ExecutionTimeRecorder](
    hookValidator:     HookValidator[F],
    statusInfoFinder:  StatusInfoFinder[F],
    projectInfoFinder: ProjectInfoFinder[F],
    elClient:          eventlog.api.events.Client[F],
    tgClient:          triplesgenerator.api.events.Client[F]
) extends Http4sDsl[F]
    with Endpoint[F] {

  import HookValidationResult._
  import hookValidator.validateHook
  import projectInfoFinder.findProjectInfo
  import statusInfoFinder.findStatusInfo
  private val executionTimeRecorder = ExecutionTimeRecorder[F]
  import executionTimeRecorder._

  def fetchProcessingStatus(projectId: GitLabId, authUser: Option[AuthUser]): F[Response[F]] =
    measureAndLogTime(logMessage(projectId)) {
      (validateHook(projectId, authUser.map(_.accessToken)) -> findStatusInfo(projectId))
        .parFlatMapN {
          case (Some(HookMissing), _)       => Ok(StatusInfo.NotActivated.asJson)
          case (Some(HookExists), Some(si)) => Ok(si.asJson)
          case (Some(HookExists), None) =>
            sendEvents(projectId, authUser) >> Ok(StatusInfo.webhookReady.widen.asJson)
          case (None, Some(si)) => Ok(si.asJson)
          case (None, None)     => NotFound(InfoMessage("Info about project cannot be found"))
        }
        .recoverWith(internalServerError(projectId))
    }

  private def sendEvents(projectId: GitLabId, authUser: Option[AuthUser]): F[Unit] =
    findProjectInfo(projectId)(authUser.map(_.accessToken)) >>= { project =>
      elClient.send(CommitSyncRequest(project)) >>
        tgClient.send(ProjectViewedEvent.forProjectAndUserId(project.path, authUser.map(_.id)))
    }

  private def internalServerError(projectId: projects.GitLabId): PartialFunction[Throwable, F[Response[F]]] = {
    case exception =>
      val message = show"Finding status info for project '$projectId' failed"
      Logger[F].error(exception)(message) >> InternalServerError(ErrorMessage(message))
  }

  private def logMessage(projectId: GitLabId): PartialFunction[Response[F], String] =
    PartialFunction.fromFunction(_ => show"Finding status info for project '$projectId' finished")
}

object Endpoint {
  def apply[F[_]: Async: NonEmptyParallel: GitLabClient: ExecutionTimeRecorder: Logger: MetricsRegistry](
      projectHookUrl: ProjectHookUrl
  ): F[Endpoint[F]] = for {
    hookValidator     <- hookvalidation.HookValidator(projectHookUrl)
    statusInfoFinder  <- StatusInfoFinder[F]
    projectInfoFinder <- ProjectInfoFinder[F]
    elClient          <- eventlog.api.events.Client[F]
    tgClient          <- triplesgenerator.api.events.Client[F]
  } yield new EndpointImpl[F](hookValidator, statusInfoFinder, projectInfoFinder, elClient, tgClient)
}
