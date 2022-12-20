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

package io.renku.webhookservice.hookcreation

import cats.effect._
import cats.syntax.all._
import io.renku.graph.model.projects.GitLabId
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.metrics.MetricsRegistry
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.hookcreation.HookCreator.CreationResult
import io.renku.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import io.renku.webhookservice.hookcreation.project.{ProjectInfo, ProjectInfoFinder}
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult
import io.renku.webhookservice.model.{CommitSyncRequest, HookToken, ProjectHookUrl}
import io.renku.webhookservice.tokenrepository.AccessTokenAssociator
import io.renku.webhookservice.{CommitSyncRequestSender, hookvalidation}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait HookCreator[F[_]] {
  def createHook(projectId: GitLabId, accessToken: AccessToken): F[CreationResult]
}

private class HookCreatorImpl[F[_]: Spawn: Logger](
    projectHookUrl:          ProjectHookUrl,
    projectHookValidator:    HookValidator[F],
    projectInfoFinder:       ProjectInfoFinder[F],
    hookTokenCrypto:         HookTokenCrypto[F],
    projectHookCreator:      ProjectHookCreator[F],
    accessTokenAssociator:   AccessTokenAssociator[F],
    commitSyncRequestSender: CommitSyncRequestSender[F]
) extends HookCreator[F] {

  import HookCreator.CreationResult._
  import accessTokenAssociator._
  import commitSyncRequestSender._
  import hookTokenCrypto._
  import projectHookCreator.create
  import projectHookValidator._
  import projectInfoFinder._

  override def createHook(projectId: GitLabId, accessToken: AccessToken): F[CreationResult] = {
    def sendCommitSyncReq(projectInfo: ProjectInfo) =
      sendCommitSyncRequest(CommitSyncRequest(projectInfo.toProject), "HookCreation")

    def createIfMissing(hvr: HookValidationResult, projectInfo: ProjectInfo): F[CreationResult] =
      hvr match {
        case HookValidationResult.HookMissing =>
          for {
            serializedHookToken <- encrypt(HookToken(projectInfo.id))
            _                   <- create(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
            _                   <- associate(projectId, accessToken)
          } yield HookCreated
        case HookValidationResult.HookExists =>
          (HookExisted: CreationResult).pure[F]
      }

    (for {
      hookValidation <- validateHook(projectId, Some(accessToken))
      projectInfo    <- findProjectInfo(projectId)(Some(accessToken))
      result         <- createIfMissing(hookValidation, projectInfo)
      _              <- Spawn[F].start(sendCommitSyncReq(projectInfo))
    } yield result).recoverWith(loggingError(projectId))
  }

  private def loggingError(projectId: GitLabId): PartialFunction[Throwable, F[CreationResult]] = {
    case NonFatal(exception) =>
      Logger[F].error(exception)(s"Hook creation failed for project with id $projectId")
      exception.raiseError[F, CreationResult]
  }
}

private object HookCreator {

  sealed trait CreationResult extends Product with Serializable
  object CreationResult {
    final case object HookCreated extends CreationResult
    final case object HookExisted extends CreationResult
  }

  case class HookAlreadyCreated(projectId: GitLabId, projectHookUrl: ProjectHookUrl)

  def apply[F[_]: Async: GitLabClient: Logger: MetricsRegistry](projectHookUrl: ProjectHookUrl,
                                                                hookTokenCrypto: HookTokenCrypto[F]
  ): F[HookCreator[F]] = for {
    commitSyncRequestSender <- CommitSyncRequestSender[F]
    hookValidator           <- hookvalidation.HookValidator(projectHookUrl)
    projectInfoFinder       <- ProjectInfoFinder[F]
    hookCreator             <- ProjectHookCreator[F]
    tokenAssociator         <- AccessTokenAssociator[F]
  } yield new HookCreatorImpl[F](
    projectHookUrl,
    hookValidator,
    projectInfoFinder,
    hookTokenCrypto,
    hookCreator,
    tokenAssociator,
    commitSyncRequestSender
  )
}
