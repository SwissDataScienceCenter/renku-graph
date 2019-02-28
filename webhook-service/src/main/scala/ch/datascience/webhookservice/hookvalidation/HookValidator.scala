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
import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.graph.tokenrepository.TokenRepositoryUrlProvider
import ch.datascience.http.client.AccessToken
import ch.datascience.logging.ApplicationLogger
import ch.datascience.webhookservice.config.GitLabConfigProvider
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult
import ch.datascience.webhookservice.project._
import ch.datascience.webhookservice.tokenrepository._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class HookValidator[Interpretation[_]](
    projectInfoFinder:     ProjectInfoFinder[Interpretation],
    projectHookUrlFinder:  ProjectHookUrlFinder[Interpretation],
    projectHookVerifier:   ProjectHookVerifier[Interpretation],
    accessTokenAssociator: AccessTokenAssociator[Interpretation],
    accessTokenRemover:    AccessTokenRemover[Interpretation],
    logger:                Logger[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable]) {

  import HookValidator.HookValidationResult._
  import ProjectVisibility._
  import projectHookUrlFinder._
  import projectHookVerifier._
  import projectInfoFinder._
  import accessTokenAssociator._
  import accessTokenRemover._

  def validateHook(projectId: ProjectId, accessToken: AccessToken): Interpretation[HookValidationResult] = {
    for {
      projectInfo             <- findProjectInfo(projectId, accessToken)
      hookUrl                 <- findProjectHookUrl
      hookPresent             <- checkProjectHookPresence(HookIdentifier(projectId, hookUrl), accessToken)
      afterVisibilityCheck    <- failIfNot(Public or Private, hookPresent, projectInfo)
      afterTokenReAssociation <- associateTokenIfPrivateAndHookExists(afterVisibilityCheck, projectInfo, accessToken)
      afterTokenDeletion      <- deleteTokenIfPrivateAndHookDoesNotExist(afterTokenReAssociation, projectInfo)
      validationResult        <- toValidationResult(afterTokenDeletion, projectId)
    } yield validationResult
  } recoverWith loggingError(projectId)

  private def failIfNot(validVisibilities:  Set[ProjectVisibility],
                        projectHookPresent: Boolean,
                        projectInfo:        ProjectInfo): Interpretation[Boolean] =
    if (validVisibilities contains projectInfo.visibility)
      ME.pure(projectHookPresent)
    else
      ME.raiseError(
        new UnsupportedOperationException(s"Hook validation not supported for '${projectInfo.visibility}' projects")
      )

  private def associateTokenIfPrivateAndHookExists(hookPresent: Boolean,
                                                   projectInfo: ProjectInfo,
                                                   accessToken: AccessToken): Interpretation[Boolean] =
    if (hookPresent && projectInfo.visibility == Private)
      associate(projectInfo.id, accessToken) map (_ => hookPresent)
    else
      ME.pure(hookPresent)

  private def deleteTokenIfPrivateAndHookDoesNotExist(hookPresent: Boolean,
                                                      projectInfo: ProjectInfo): Interpretation[Boolean] =
    if (!hookPresent && projectInfo.visibility == Private)
      removeAccessToken(projectInfo.id) map (_ => false)
    else
      ME.pure(hookPresent)

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

  private implicit class ReadablityImprover(visibility: ProjectVisibility) {
    def or(orVisibility: ProjectVisibility): Set[ProjectVisibility] =
      Set(visibility, orVisibility)
  }
}

object HookValidator {

  sealed trait HookValidationResult
  object HookValidationResult {
    final case object HookExists  extends HookValidationResult
    final case object HookMissing extends HookValidationResult
  }
}

class IOHookValidator(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO])
    extends HookValidator[IO](
      new IOProjectInfoFinder(new GitLabConfigProvider[IO]),
      new IOProjectHookUrlFinder,
      new IOProjectHookVerifier(new GitLabConfigProvider[IO]),
      new IOAccessTokenAssociator(new TokenRepositoryUrlProvider[IO]),
      new IOAccessTokenRemover(new TokenRepositoryUrlProvider[IO]),
      ApplicationLogger
    )
