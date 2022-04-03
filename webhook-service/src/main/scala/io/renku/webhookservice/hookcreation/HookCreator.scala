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

import cats.data.EitherT
import cats.effect._
import cats.syntax.all._
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.model.projects.Id
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.metrics.MetricsRegistry
import io.renku.webhookservice.crypto.HookTokenCrypto
import io.renku.webhookservice.hookcreation.HookCreator.{CreationResult, HookAlreadyCreated}
import io.renku.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import io.renku.webhookservice.hookcreation.project.ProjectInfoFinder
import io.renku.webhookservice.hookvalidation.HookValidator
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult.HookMissing
import io.renku.webhookservice.model.{CommitSyncRequest, HookToken, ProjectHookUrl}
import io.renku.webhookservice.tokenrepository.AccessTokenAssociator
import io.renku.webhookservice.{CommitSyncRequestSender, hookvalidation}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait HookCreator[F[_]] {
  def createHook(projectId: Id, accessToken: AccessToken): F[CreationResult]
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

  override def createHook(projectId: Id, accessToken: AccessToken): F[CreationResult] = {
    for {
      hookValidation      <- right(validateHook(projectId, Some(accessToken)))
      _                   <- leftIfProjectHookExists(hookValidation, projectId, projectHookUrl)
      projectInfo         <- right(findProjectInfo(projectId, Some(accessToken)))
      serializedHookToken <- right(encrypt(HookToken(projectInfo.id)))
      _                   <- right(create(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken))
      _                   <- right(associate(projectId, accessToken))
      _ <- right(Spawn[F].start(sendCommitSyncRequest(CommitSyncRequest(projectInfo.toProject), "HookCreation")))
    } yield ()
  }.fold[CreationResult](_ => HookExisted, _ => HookCreated) recoverWith loggingError(projectId)

  private def leftIfProjectHookExists(
      hookValidation: HookValidationResult,
      projectId:      Id,
      projectHookUrl: ProjectHookUrl
  ): EitherT[F, HookAlreadyCreated, Unit] = EitherT.cond[F](
    test = hookValidation == HookMissing,
    left = HookAlreadyCreated(projectId, projectHookUrl),
    right = ()
  )

  private def loggingError(projectId: Id): PartialFunction[Throwable, F[CreationResult]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(s"Hook creation failed for project with id $projectId")
    exception.raiseError[F, CreationResult]
  }

  private def right[T](value: F[T]): EitherT[F, HookAlreadyCreated, T] =
    EitherT.right[HookAlreadyCreated](value)
}

private object HookCreator {

  sealed trait CreationResult extends Product with Serializable
  object CreationResult {
    final case object HookCreated extends CreationResult
    final case object HookExisted extends CreationResult
  }

  case class HookAlreadyCreated(projectId: Id, projectHookUrl: ProjectHookUrl)

  def apply[F[_]: Async: Logger: MetricsRegistry](
      projectHookUrl:  ProjectHookUrl,
      gitLabClient:    GitLabClient[F],
      gitLabThrottler: Throttler[F, GitLab],
      hookTokenCrypto: HookTokenCrypto[F]
  ): F[HookCreator[F]] =
    for {
      commitSyncRequestSender <- CommitSyncRequestSender[F]
      hookValidator           <- hookvalidation.HookValidator(projectHookUrl, gitLabThrottler)
      projectInfoFinder       <- ProjectInfoFinder[F](gitLabThrottler)
      hookCreator             <- ProjectHookCreator[F](gitLabClient)
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
