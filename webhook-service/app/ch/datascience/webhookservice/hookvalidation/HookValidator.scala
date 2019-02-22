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

package ch.datascience.webhookservice.hookvalidation

import ProjectHookVerifier.HookIdentifier
import cats.MonadError
import cats.effect.IO
import cats.implicits._
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.IOLogger
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult
import ch.datascience.webhookservice.model.ProjectInfo
import ch.datascience.webhookservice.project._
import io.chrisdavenport.log4cats.Logger
import javax.inject.{Inject, Singleton}

import scala.language.higherKinds
import scala.util.control.NonFatal

class HookValidator[Interpretation[_]](
    projectInfoFinder:    ProjectInfoFinder[Interpretation],
    projectHookUrlFinder: ProjectHookUrlFinder[Interpretation],
    projectHookVerifier:  ProjectHookVerifier[Interpretation],
    logger:               Logger[Interpretation]
)(implicit ME:            MonadError[Interpretation, Throwable]) {

  import HookValidator.HookValidationResult._
  import ch.datascience.webhookservice.model.ProjectVisibility._
  import projectHookUrlFinder._
  import projectHookVerifier._
  import projectInfoFinder._

  def validateHook(projectId: ProjectId, accessToken: AccessToken): Interpretation[HookValidationResult] = {
    for {
      projectInfo                 <- findProjectInfo(projectId, accessToken)
      hookUrl                     <- findProjectHookUrl
      hookPresent                 <- checkProjectHookPresence(HookIdentifier(projectId, hookUrl), accessToken)
      afterVisibilityCheckPresent <- failIfNonPublicProject(hookPresent, projectInfo)
      validationResult            <- toValidationResult(afterVisibilityCheckPresent, projectId)
    } yield validationResult
  } recoverWith loggingError(projectId)

  private def failIfNonPublicProject(projectHookPresent: Boolean, projectInfo: ProjectInfo): Interpretation[Boolean] =
    if (projectInfo.visibility == Public) ME.pure(projectHookPresent)
    else ME.raiseError(new NotImplementedError("Hook validation does not work for private projects"))

  private def toValidationResult(projectHookPresent: Boolean,
                                 projectId:          ProjectId): Interpretation[HookValidationResult] =
    if (projectHookPresent)
      logger.info(s"Hook exists for project with id $projectId").map(_ => HookExists)
    else
      logger.info(s"Hook missing for project with id $projectId").map(_ => HookMissing)

  private def loggingError(projectId: ProjectId): PartialFunction[Throwable, Interpretation[HookValidationResult]] = {
    case NonFatal(exception) =>
      logger.error(exception)(s"Hook validation fails for project with id $projectId")
      ME.raiseError(exception)
  }
}

object HookValidator {

  sealed trait HookValidationResult
  object HookValidationResult {
    final case object HookExists  extends HookValidationResult
    final case object HookMissing extends HookValidationResult
  }
}

@Singleton
class IOHookValidator @Inject()(
    projectInfoFinder:    IOProjectInfoFinder,
    projectHookUrlFinder: IOProjectHookUrlFinder,
    projectHookVerifier:  IOProjectHookVerifier,
    logger:               IOLogger
) extends HookValidator[IO](projectInfoFinder, projectHookUrlFinder, projectHookVerifier, logger)
