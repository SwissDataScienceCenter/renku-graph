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

import cats.data.EitherT
import cats.effect._
import cats.implicits._
import cats.{Monad, MonadError}
import ch.datascience.clients.AccessToken
import ch.datascience.graph.events.ProjectId
import ch.datascience.logging.IOLogger
import ch.datascience.webhookservice.crypto.{HookTokenCrypto, IOHookTokenCrypto}
import ch.datascience.webhookservice.hookcreation.HookCreator.{HookAlreadyCreated, HookCreationResult}
import ch.datascience.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult.HookMissing
import ch.datascience.webhookservice.hookvalidation.{HookValidator, IOHookValidator}
import ch.datascience.webhookservice.model.HookToken
import ch.datascience.webhookservice.project.ProjectHookUrlFinder.ProjectHookUrl
import ch.datascience.webhookservice.project._
import io.chrisdavenport.log4cats.Logger
import javax.inject.{Inject, Singleton}

import scala.language.higherKinds
import scala.util.control.NonFatal

private class HookCreator[Interpretation[_]: Monad](
    projectHookUrlFinder: ProjectHookUrlFinder[Interpretation],
    projectHookValidator: HookValidator[Interpretation],
    projectInfoFinder:    ProjectInfoFinder[Interpretation],
    hookTokenCrypto:      HookTokenCrypto[Interpretation],
    projectHookCreator:   ProjectHookCreator[Interpretation],
    eventsHistoryLoader:  EventsHistoryLoader[Interpretation],
    logger:               Logger[Interpretation]
)(implicit ME:            MonadError[Interpretation, Throwable]) {

  import HookCreator.HookCreationResult._
  import hookTokenCrypto._
  import projectHookCreator.create
  import projectHookUrlFinder._
  import projectHookValidator._
  import projectInfoFinder._

  def createHook(projectId: ProjectId, accessToken: AccessToken): Interpretation[HookCreationResult] = {
    for {
      projectHookUrl      <- right(findProjectHookUrl)
      hookValidation      <- right(validateHook(projectId, accessToken))
      _                   <- leftIfProjectHookExists(hookValidation, projectId, projectHookUrl)
      projectInfo         <- right(findProjectInfo(projectId, accessToken))
      serializedHookToken <- right(encrypt(HookToken(projectInfo.id)))
      _                   <- right(create(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken))
      _                   <- right(eventsHistoryLoader.loadAllEvents(projectInfo, accessToken))
    } yield ()
  } fold (leftToHookExisted, rightToHookCreated(projectId)) recoverWith loggingError(projectId)

  private def leftIfProjectHookExists(
      hookValidation: HookValidationResult,
      projectId:      ProjectId,
      projectHookUrl: ProjectHookUrl): EitherT[Interpretation, HookAlreadyCreated, Unit] = EitherT.cond[Interpretation](
    test  = hookValidation == HookMissing,
    left  = HookAlreadyCreated(projectId, projectHookUrl),
    right = ()
  )

  private lazy val leftToHookExisted: HookAlreadyCreated => HookCreationResult = hookAlreadyCreated => {
    logger.info(
      s"Hook already created for projectId: ${hookAlreadyCreated.projectId}, url: ${hookAlreadyCreated.projectHookUrl}"
    )
    HookExisted
  }

  private def rightToHookCreated(projectId: ProjectId): Unit => HookCreationResult = _ => {
    logger.info(s"Hook created for project with id $projectId")
    HookCreated
  }

  private def loggingError(projectId: ProjectId): PartialFunction[Throwable, Interpretation[HookCreationResult]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"Hook creation failed for project with id $projectId")
      ME.raiseError(exception)
  }

  private def right[T](value: Interpretation[T]): EitherT[Interpretation, HookAlreadyCreated, T] =
    EitherT.right[HookAlreadyCreated](value)
}

private object HookCreator {

  sealed trait HookCreationResult
  object HookCreationResult {
    final case object HookCreated extends HookCreationResult
    final case object HookExisted extends HookCreationResult
  }

  private case class HookAlreadyCreated(
      projectId:      ProjectId,
      projectHookUrl: ProjectHookUrl
  )
}

@Singleton
private class IOHookCreator @Inject()(
    projectHookUrlFinder: IOProjectHookUrlFinder,
    projectHookValidator: IOHookValidator,
    projectInfoFinder:    IOProjectInfoFinder,
    hookTokenCrypto:      IOHookTokenCrypto,
    projectHookCreator:   IOProjectHookCreator,
    eventsHistoryLoader:  IOEventsHistoryLoader,
    logger:               IOLogger
) extends HookCreator[IO](
      projectHookUrlFinder,
      projectHookValidator,
      projectInfoFinder,
      hookTokenCrypto,
      projectHookCreator,
      eventsHistoryLoader,
      logger
    )
