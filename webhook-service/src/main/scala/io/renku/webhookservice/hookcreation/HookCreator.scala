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

package io.renku.webhookservice.hookcreation

import cats.Show
import cats.effect._
import cats.syntax.all._
import io.renku.graph.eventlog
import io.renku.graph.eventlog.api.events.CommitSyncRequest
import io.renku.graph.model.projects
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.metrics.MetricsRegistry
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.hookcreation.HookCreator.CreationResult
import io.renku.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult
import io.renku.webhookservice.model._
import io.renku.webhookservice.tokenrepository.AccessTokenAssociator
import io.renku.webhookservice.{ProjectInfoFinder, hookvalidation}
import org.typelevel.log4cats.Logger

private trait HookCreator[F[_]] {
  def createHook(projectId: GitLabId, accessToken: AccessToken): F[CreationResult]
}

private class HookCreatorImpl[F[_]: Spawn: Logger](
    projectHookUrl:        ProjectHookUrl,
    projectHookValidator:  HookValidator[F],
    projectInfoFinder:     ProjectInfoFinder[F],
    hookTokenCrypto:       HookTokenCrypto[F],
    projectHookCreator:    ProjectHookCreator[F],
    accessTokenAssociator: AccessTokenAssociator[F],
    elClient:              eventlog.api.events.Client[F]
) extends HookCreator[F] {

  import HookCreator.CreationResult._
  import accessTokenAssociator._
  import hookTokenCrypto._
  import projectHookCreator.create
  import projectHookValidator._
  import projectInfoFinder.findProjectInfo

  override def createHook(projectId: GitLabId, accessToken: AccessToken): F[CreationResult] = {

    def createIfMissing(hvr: HookValidationResult): F[CreationResult] =
      hvr match {
        case HookValidationResult.HookMissing =>
          for {
            _                   <- associate(projectId, accessToken)
            serializedHookToken <- encrypt(HookToken(projectId))
            _                   <- create(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
          } yield HookCreated
        case HookValidationResult.HookExists =>
          associate(projectId, accessToken).map(_ => HookExisted.widen)
      }

    {
      for {
        validationResult <- validateHook(projectId, accessToken.some)
        creationResult   <- createIfMissing(validationResult)
        _                <- Logger[F].info(show"Hook $creationResult for projectId $projectId")
        _                <- Spawn[F].start(sendCommitSyncReq(projectId, accessToken))
      } yield creationResult
    }.onError(loggingError(projectId))
  }

  private def sendCommitSyncReq(projectId: projects.GitLabId, accessToken: AccessToken) =
    findProjectInfo(projectId)(accessToken.some)
      .onError(
        Logger[F].error(_)(
          s"Hook creation - sending COMMIT_SYNC_REQUEST failure; finding project info for projectId $projectId failed"
        )
      )
      .map(CommitSyncRequest(_))
      .flatMap(elClient.send)

  private def loggingError(projectId: GitLabId): PartialFunction[Throwable, F[Unit]] = { case exception =>
    Logger[F].error(exception)(s"Hook creation failed for project with id $projectId")
  }
}

private object HookCreator {

  sealed trait CreationResult extends Product {
    lazy val widen: CreationResult = this
  }
  object CreationResult {
    final case object HookCreated extends CreationResult
    final case object HookExisted extends CreationResult

    implicit val show: Show[CreationResult] = Show {
      case HookCreated => "created"
      case HookExisted => "existed"
    }
  }

  def apply[F[_]: Async: GitLabClient: Logger: MetricsRegistry](projectHookUrl:  ProjectHookUrl,
                                                                hookTokenCrypto: HookTokenCrypto[F]
  ): F[HookCreator[F]] = for {
    hookValidator     <- hookvalidation.HookValidator(projectHookUrl)
    projectInfoFinder <- ProjectInfoFinder[F]
    hookCreator       <- ProjectHookCreator[F]
    tokenAssociator   <- AccessTokenAssociator[F]
    elClient          <- eventlog.api.events.Client[F]
  } yield new HookCreatorImpl[F](
    projectHookUrl,
    hookValidator,
    projectInfoFinder,
    hookTokenCrypto,
    hookCreator,
    tokenAssociator,
    elClient
  )
}
