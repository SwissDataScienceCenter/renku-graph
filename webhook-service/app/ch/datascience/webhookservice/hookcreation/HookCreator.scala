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

import cats.effect._
import cats.implicits._
import cats.{Monad, MonadError}
import ch.datascience.clients.AccessToken
import ch.datascience.graph.events.ProjectId
import ch.datascience.logging.IOLogger
import ch.datascience.webhookservice.crypto.{HookTokenCrypto, IOHookTokenCrypto}
import ch.datascience.webhookservice.hookcreation.HookCreator.HookAlreadyCreated
import ch.datascience.webhookservice.hookcreation.ProjectHookCreator.ProjectHook
import ch.datascience.webhookservice.hookcreation.ProjectHookUrlFinder.ProjectHookUrl
import ch.datascience.webhookservice.hookcreation.ProjectHookVerifier.HookIdentifier
import ch.datascience.webhookservice.model.HookToken
import io.chrisdavenport.log4cats.Logger
import javax.inject.{Inject, Singleton}

import scala.language.higherKinds
import scala.util.control.NonFatal

private class HookCreator[Interpretation[_]: Monad](
    projectHookUrlFinder:    ProjectHookUrlFinder[Interpretation],
    projectHookVerifier:     ProjectHookVerifier[Interpretation],
    projectInfoFinder:       ProjectInfoFinder[Interpretation],
    hookAccessTokenVerifier: HookAccessTokenVerifier[Interpretation],
    hookAccessTokenCreator:  HookAccessTokenCreator[Interpretation],
    hookTokenCrypto:         HookTokenCrypto[Interpretation],
    projectHookCreator:      ProjectHookCreator[Interpretation],
    eventsHistoryLoader:     EventsHistoryLoader[Interpretation],
    logger:                  Logger[Interpretation]
)(implicit ME:               MonadError[Interpretation, Throwable]) {

  import hookAccessTokenCreator._
  import hookAccessTokenVerifier._
  import hookTokenCrypto._
  import projectHookUrlFinder._
  import projectHookVerifier._
  import projectInfoFinder._

  def createHook(projectId: ProjectId, accessToken: AccessToken): Interpretation[Unit] = {
    for {
      projectHookUrl          <- findProjectHookUrl
      projectHookPresence     <- checkProjectHookPresence(HookIdentifier(projectId, projectHookUrl), accessToken)
      _                       <- failIfProjectHookExists(projectId, projectHookUrl)(projectHookPresence)
      projectInfo             <- findProjectInfo(projectId, accessToken)
      hookAccessTokenPresence <- checkHookAccessTokenPresence(projectInfo, accessToken)
      _                       <- failIfHookAccessTokenExists(projectId)(hookAccessTokenPresence)
      hookAccessToken         <- createHookAccessToken(projectInfo, accessToken)
      serializedHookToken     <- encrypt(HookToken(projectInfo.id, hookAccessToken))
      _                       <- projectHookCreator.createHook(ProjectHook(projectId, projectHookUrl, serializedHookToken), accessToken)
      _                       <- logger.info(s"Hook created for project with id $projectId")
      _                       <- eventsHistoryLoader.loadAllEvents(projectInfo, hookAccessToken, accessToken) recover withSuccess
    } yield ()
  } recoverWith loggingError(projectId)

  private def failIfProjectHookExists(projectId:      ProjectId,
                                      projectHookUrl: ProjectHookUrl): Boolean => Interpretation[Unit] = {
    case true  => ME.raiseError(HookAlreadyCreated(projectId, projectHookUrl))
    case false => ME.pure(())
  }

  private def failIfHookAccessTokenExists(projectId: ProjectId): Boolean => Interpretation[Unit] = {
    case true =>
      ME.raiseError(
        new RuntimeException(s"Personal Access Token already exists for hook of the project with id $projectId")
      )
    case false => ME.pure(())
  }

  private def loggingError(projectId: ProjectId): PartialFunction[Throwable, Interpretation[Unit]] = {
    case exception @ HookAlreadyCreated(_, _) =>
      logger.warn(exception.getMessage)
      ME.raiseError(exception)
    case NonFatal(exception) =>
      logger.error(exception)(s"Hook creation failed for project with id $projectId")
      ME.raiseError(exception)
  }

  private lazy val withSuccess: PartialFunction[Throwable, Unit] = {
    case NonFatal(_) => ()
  }
}

private object HookCreator {

  final case class HookAlreadyCreated(
      projectId:      ProjectId,
      projectHookUrl: ProjectHookUrl
  ) extends RuntimeException(s"Hook already created for projectId: $projectId, url: $projectHookUrl")
}

@Singleton
private class IOHookCreator @Inject()(
    projectHookUrlFinder:    IOProjectHookUrlFinder,
    projectHookVerifier:     IOProjectHookVerifier,
    projectInfoFinder:       IOProjectInfoFinder,
    hookAccessTokenVerifier: IOHookAccessTokenVerifier,
    hookAccessTokenCreator:  IOHookAccessTokenCreator,
    hookTokenCrypto:         IOHookTokenCrypto,
    projectHookCreator:      IOProjectHookCreator,
    eventsHistoryLoader:     IOEventsHistoryLoader,
    logger:                  IOLogger
) extends HookCreator[IO](
      projectHookUrlFinder,
      projectHookVerifier,
      projectInfoFinder,
      hookAccessTokenVerifier,
      hookAccessTokenCreator,
      hookTokenCrypto,
      projectHookCreator,
      eventsHistoryLoader,
      logger
    )
