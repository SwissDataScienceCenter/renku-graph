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
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder, TokenRepositoryUrlProvider}
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError.UnauthorizedException
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
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    accessTokenAssociator: AccessTokenAssociator[Interpretation],
    accessTokenRemover:    AccessTokenRemover[Interpretation],
    logger:                Logger[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable]) {

  import HookValidator.HookValidationResult._
  import ProjectVisibility._
  import Token._
  import accessTokenAssociator._
  import accessTokenFinder._
  import accessTokenRemover._
  import projectHookUrlFinder._
  import projectHookVerifier._
  import projectInfoFinder._

  def validateHook(projectId: ProjectId, accessToken: AccessToken): Interpretation[HookValidationResult] =
    findVisibilityAndToken(projectId, accessToken) flatMap {
      case (Public, token) =>
        for {
          hookUrl          <- findProjectHookUrl
          hookPresent      <- checkHookPresence(HookIdentifier(projectId, hookUrl), token.value)
          validationResult <- toValidationResult(hookPresent, projectId)
        } yield validationResult
      case (_: TokenProtectedProject, GivenToken(token)) =>
        for {
          hookUrl          <- findProjectHookUrl
          hookPresent      <- checkHookPresence(HookIdentifier(projectId, hookUrl), token)
          _                <- if (hookPresent) associate(projectId, token) else ME.unit
          _                <- if (!hookPresent) removeAccessToken(projectId) else ME.unit
          validationResult <- toValidationResult(hookPresent, projectId)
        } yield validationResult
      case (_: TokenProtectedProject, StoredToken(token)) =>
        for {
          hookUrl          <- findProjectHookUrl
          hookPresent      <- checkHookPresence(HookIdentifier(projectId, hookUrl), token)
          _                <- if (!hookPresent) removeAccessToken(projectId) else ME.unit
          validationResult <- toValidationResult(hookPresent, projectId)
        } yield validationResult
    } recoverWith loggingError(projectId)

  private def findVisibilityAndToken(projectId:   ProjectId,
                                     accessToken: AccessToken): Interpretation[(ProjectVisibility, Token)] =
    findProjectInfo(projectId, accessToken)
      .map(_.visibility -> (GivenToken(accessToken): Token))
      .recoverWith(visibilityAndStoredToken(projectId))

  private def visibilityAndStoredToken(
      projectId: ProjectId
  ): PartialFunction[Throwable, Interpretation[(ProjectVisibility, Token)]] = {
    case UnauthorizedException => {
      for {
        storedAccessToken <- findAccessToken(projectId) flatMap getOrError(projectId)
        projectInfo       <- findProjectInfo(projectId, storedAccessToken.value)
      } yield projectInfo.visibility -> storedAccessToken
    } recoverWith storedAccessTokenError(projectId)
  }

  private def getOrError(projectId: ProjectId): Option[AccessToken] => Interpretation[Token] = {
    case Some(token) => ME.pure(StoredToken(token))
    case None        => ME.raiseError[Token](new Exception(s"No access token found for projectId $projectId"))
  }

  private def storedAccessTokenError(
      projectId: ProjectId
  ): PartialFunction[Throwable, Interpretation[(ProjectVisibility, Token)]] = {
    case UnauthorizedException => ME.raiseError(new Exception(s"Stored access token for $projectId is invalid"))
  }

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

  private sealed abstract class Token(val value: AccessToken)
  private object Token {
    final case class GivenToken(override val value:  AccessToken) extends Token(value)
    final case class StoredToken(override val value: AccessToken) extends Token(value)
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
      new IOAccessTokenFinder(new TokenRepositoryUrlProvider[IO]),
      new IOAccessTokenAssociator(new TokenRepositoryUrlProvider[IO]),
      new IOAccessTokenRemover(new TokenRepositoryUrlProvider[IO]),
      ApplicationLogger
    )
