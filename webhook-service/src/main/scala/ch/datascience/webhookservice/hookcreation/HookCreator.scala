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

package ch.datascience.webhookservice.hookcreation

import cats.MonadError
import cats.data.EitherT
import cats.effect._
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.projects.Id
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.webhookservice.CommitSyncRequestSender
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.hookcreation.HookCreator.{CreationResult, HookAlreadyCreated}
import ch.datascience.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import ch.datascience.webhookservice.hookcreation.project._
import ch.datascience.webhookservice.hookvalidation.HookValidator
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult.HookMissing
import ch.datascience.webhookservice.model.{CommitSyncRequest, HookToken, ProjectHookUrl}
import ch.datascience.webhookservice.tokenrepository.{AccessTokenAssociator, IOAccessTokenAssociator}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private trait HookCreator[Interpretation[_]] {
  def createHook(projectId: Id, accessToken: AccessToken): Interpretation[CreationResult]
}

private class HookCreatorImpl[Interpretation[_]: MonadError[*[_], Throwable]](
    projectHookUrl:          ProjectHookUrl,
    projectHookValidator:    HookValidator[Interpretation],
    projectInfoFinder:       ProjectInfoFinder[Interpretation],
    hookTokenCrypto:         HookTokenCrypto[Interpretation],
    projectHookCreator:      ProjectHookCreator[Interpretation],
    accessTokenAssociator:   AccessTokenAssociator[Interpretation],
    commitSyncRequestSender: CommitSyncRequestSender[Interpretation],
    logger:                  Logger[Interpretation]
)(implicit
    contextShift: ContextShift[Interpretation],
    concurrent:   Concurrent[Interpretation]
) extends HookCreator[Interpretation] {

  import HookCreator.CreationResult._
  import accessTokenAssociator._
  import commitSyncRequestSender._
  import hookTokenCrypto._
  import projectHookCreator.create
  import projectHookValidator._
  import projectInfoFinder._

  override def createHook(projectId: Id, accessToken: AccessToken): Interpretation[CreationResult] = {
    for {
      hookValidation      <- right(validateHook(projectId, Some(accessToken)))
      _                   <- leftIfProjectHookExists(hookValidation, projectId, projectHookUrl)
      projectInfo         <- right(findProjectInfo(projectId, Some(accessToken)))
      serializedHookToken <- right(encrypt(HookToken(projectInfo.id)))
      _                   <- right(create(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken))
      _                   <- right(associate(projectId, accessToken))
      _ <- right(
             contextShift.shift *> concurrent.start(sendCommitSyncRequest(CommitSyncRequest(projectInfo.toProject)))
           )
    } yield ()
  }.fold[CreationResult](_ => HookExisted, _ => HookCreated) recoverWith loggingError(projectId)

  private def leftIfProjectHookExists(
      hookValidation: HookValidationResult,
      projectId:      Id,
      projectHookUrl: ProjectHookUrl
  ): EitherT[Interpretation, HookAlreadyCreated, Unit] = EitherT.cond[Interpretation](
    test = hookValidation == HookMissing,
    left = HookAlreadyCreated(projectId, projectHookUrl),
    right = ()
  )

  private def loggingError(projectId: Id): PartialFunction[Throwable, Interpretation[CreationResult]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"Hook creation failed for project with id $projectId")
      exception.raiseError[Interpretation, CreationResult]
  }

  private def right[T](value: Interpretation[T]): EitherT[Interpretation, HookAlreadyCreated, T] =
    EitherT.right[HookAlreadyCreated](value)
}

private object HookCreator {

  sealed trait CreationResult extends Product with Serializable
  object CreationResult {
    final case object HookCreated extends CreationResult
    final case object HookExisted extends CreationResult
  }

  case class HookAlreadyCreated(projectId: Id, projectHookUrl: ProjectHookUrl)

  def apply(
      projectHookUrl:        ProjectHookUrl,
      gitLabThrottler:       Throttler[IO, GitLab],
      hookTokenCrypto:       HookTokenCrypto[IO],
      executionTimeRecorder: ExecutionTimeRecorder[IO],
      logger:                Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      clock:            Clock[IO],
      timer:            Timer[IO]
  ): IO[HookCreator[IO]] =
    for {
      commitSyncRequestSender <- CommitSyncRequestSender(logger)
      hookValidator           <- HookValidator(projectHookUrl, gitLabThrottler)
      projectInfoFinder       <- ProjectInfoFinder(gitLabThrottler, logger)
      hookCreator             <- IOProjectHookCreator(gitLabThrottler, logger)
      tokenAssociator         <- IOAccessTokenAssociator(logger)
    } yield new HookCreatorImpl[IO](
      projectHookUrl,
      hookValidator,
      projectInfoFinder,
      hookTokenCrypto,
      hookCreator,
      tokenAssociator,
      commitSyncRequestSender,
      logger
    )
}
