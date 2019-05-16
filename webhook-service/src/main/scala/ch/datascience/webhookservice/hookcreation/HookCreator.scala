/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import cats.implicits._
import ch.datascience.control.Throttler
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.graph.gitlab.GitLab
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.graph.tokenrepository.TokenRepositoryUrlProvider
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ApplicationLogger
import ch.datascience.webhookservice.config.GitLabConfigProvider
import ch.datascience.webhookservice.crypto.HookTokenCrypto
import ch.datascience.webhookservice.hookcreation.HookCreator.{CreationResult, HookAlreadyCreated}
import ch.datascience.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult.HookMissing
import ch.datascience.webhookservice.hookvalidation.{HookValidator, IOHookValidator}
import ch.datascience.webhookservice.model.HookToken
import ch.datascience.webhookservice.project.ProjectHookUrlFinder.ProjectHookUrl
import ch.datascience.webhookservice.project._
import ch.datascience.webhookservice.tokenrepository.{AccessTokenAssociator, IOAccessTokenAssociator}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

private class HookCreator[Interpretation[_]](
    projectHookUrlFinder:  ProjectHookUrlFinder[Interpretation],
    projectHookValidator:  HookValidator[Interpretation],
    projectInfoFinder:     ProjectInfoFinder[Interpretation],
    hookTokenCrypto:       HookTokenCrypto[Interpretation],
    projectHookCreator:    ProjectHookCreator[Interpretation],
    accessTokenAssociator: AccessTokenAssociator[Interpretation],
    eventsHistoryLoader:   EventsHistoryLoader[Interpretation],
    logger:                Logger[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable],
  contextShift:            ContextShift[Interpretation],
  concurrent:              Concurrent[Interpretation]) {

  import HookCreator.CreationResult._
  import accessTokenAssociator._
  import hookTokenCrypto._
  import projectHookCreator.create
  import projectHookUrlFinder._
  import projectHookValidator._
  import projectInfoFinder._

  def createHook(projectId: ProjectId, accessToken: AccessToken): Interpretation[CreationResult] = {
    for {
      projectHookUrl      <- right(findProjectHookUrl)
      hookValidation      <- right(validateHook(projectId, Some(accessToken)))
      _                   <- leftIfProjectHookExists(hookValidation, projectId, projectHookUrl)
      projectInfo         <- right(findProjectInfo(projectId, Some(accessToken)))
      serializedHookToken <- right(encrypt(HookToken(projectInfo.id)))
      _                   <- right(create(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken))
      _                   <- right(associate(projectId, accessToken))
      _                   <- right(contextShift.shift *> concurrent.start(eventsHistoryLoader.loadAllEvents(projectInfo, accessToken)))
    } yield ()
  } fold [CreationResult] (_ => HookExisted, _ => HookCreated) recoverWith loggingError(projectId)

  private def leftIfProjectHookExists(
      hookValidation: HookValidationResult,
      projectId:      ProjectId,
      projectHookUrl: ProjectHookUrl): EitherT[Interpretation, HookAlreadyCreated, Unit] = EitherT.cond[Interpretation](
    test  = hookValidation == HookMissing,
    left  = HookAlreadyCreated(projectId, projectHookUrl),
    right = ()
  )

  private def loggingError(projectId: ProjectId): PartialFunction[Throwable, Interpretation[CreationResult]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"Hook creation failed for project with id $projectId")
      ME.raiseError(exception)
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

  private case class HookAlreadyCreated(
      projectId:      ProjectId,
      projectHookUrl: ProjectHookUrl
  )
}

private class IOHookCreator(
    transactor:              DbTransactor[IO, EventLogDB],
    gitLabThrottler:         Throttler[IO, GitLab]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], clock: Clock[IO], timer: Timer[IO])
    extends HookCreator[IO](
      new IOProjectHookUrlFinder,
      new IOHookValidator(gitLabThrottler),
      new IOProjectInfoFinder(new GitLabConfigProvider[IO], gitLabThrottler, ApplicationLogger),
      HookTokenCrypto[IO],
      new IOProjectHookCreator(new GitLabConfigProvider[IO], gitLabThrottler, ApplicationLogger),
      new IOAccessTokenAssociator(new TokenRepositoryUrlProvider[IO](), ApplicationLogger),
      new IOEventsHistoryLoader(transactor, gitLabThrottler),
      ApplicationLogger
    )
