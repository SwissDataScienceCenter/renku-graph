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

package ch.datascience.webhookservice.hookvalidation

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import cats.{Applicative, MonadError}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.projects.Id
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, AccessTokenFinder, TokenRepositoryUrl}
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.logging.ApplicationLogger
import ch.datascience.webhookservice.hookvalidation.HookValidator.HookValidationResult
import ch.datascience.webhookservice.hookvalidation.ProjectHookVerifier.HookIdentifier
import ch.datascience.webhookservice.model.ProjectHookUrl
import ch.datascience.webhookservice.tokenrepository._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait HookValidator[Interpretation[_]] {
  def validateHook(projectId: Id, maybeAccessToken: Option[AccessToken]): Interpretation[HookValidationResult]
}

class HookValidatorImpl[Interpretation[_]: MonadError[*[_], Throwable]](
    projectHookUrl:        ProjectHookUrl,
    projectHookVerifier:   ProjectHookVerifier[Interpretation],
    accessTokenFinder:     AccessTokenFinder[Interpretation],
    accessTokenAssociator: AccessTokenAssociator[Interpretation],
    accessTokenRemover:    AccessTokenRemover[Interpretation],
    logger:                Logger[Interpretation]
) extends HookValidator[Interpretation] {

  private val applicative = Applicative[Interpretation]

  import HookValidator.HookValidationResult._
  import HookValidator._
  import AccessTokenFinder._
  import Token._
  import accessTokenAssociator._
  import accessTokenFinder._
  import accessTokenRemover._
  import applicative._
  import projectHookVerifier._

  def validateHook(projectId: Id, maybeAccessToken: Option[AccessToken]): Interpretation[HookValidationResult] =
    findToken(projectId, maybeAccessToken) flatMap {
      case GivenToken(token) =>
        for {
          hookPresent      <- checkHookPresence(HookIdentifier(projectId, projectHookUrl), token)
          _                <- whenA(hookPresent)(associate(projectId, token))
          _                <- whenA(!hookPresent)(removeAccessToken(projectId))
          validationResult <- toValidationResult(hookPresent).pure[Interpretation]
        } yield validationResult
      case StoredToken(token) =>
        for {
          hookPresent      <- checkHookPresence(HookIdentifier(projectId, projectHookUrl), token)
          _                <- whenA(!hookPresent)(removeAccessToken(projectId))
          validationResult <- toValidationResult(hookPresent).pure[Interpretation]
        } yield validationResult
    } recoverWith loggingError(projectId)

  private def findToken(
      projectId:        Id,
      maybeAccessToken: Option[AccessToken]
  ): Interpretation[Token] =
    maybeAccessToken
      .map(token => (GivenToken(token): Token).pure[Interpretation])
      .getOrElse(findStoredToken(projectId))

  private def findStoredToken(projectId: Id): Interpretation[Token] =
    findAccessToken(projectId) flatMap getOrError(projectId) recoverWith storedAccessTokenError(projectId)

  private def getOrError(projectId: Id): Option[AccessToken] => Interpretation[Token] = {
    case Some(token) =>
      (StoredToken(token): Token).pure[Interpretation]
    case None =>
      NoAccessTokenException(s"No access token found for projectId $projectId").raiseError[Interpretation, Token]
  }

  private def storedAccessTokenError(
      projectId: Id
  ): PartialFunction[Throwable, Interpretation[Token]] = { case UnauthorizedException =>
    new Exception(s"Stored access token for $projectId is invalid").raiseError[Interpretation, Token]
  }

  private def toValidationResult(projectHookPresent: Boolean): HookValidationResult =
    if (projectHookPresent) HookExists else HookMissing

  private def loggingError(projectId: Id): PartialFunction[Throwable, Interpretation[HookValidationResult]] = {
    case exception @ NoAccessTokenException(message) =>
      logger.info(s"Hook validation failed: $message")
      exception.raiseError[Interpretation, HookValidationResult]
    case NonFatal(exception) =>
      logger.error(exception)(s"Hook validation failed for project with id $projectId")
      exception.raiseError[Interpretation, HookValidationResult]
  }

  private sealed abstract class Token(val value: AccessToken)
  private object Token {
    case class GivenToken(override val value: AccessToken) extends Token(value)
    case class StoredToken(override val value: AccessToken) extends Token(value)
  }
}

object HookValidator {

  sealed trait HookValidationResult
  object HookValidationResult {
    final case object HookExists  extends HookValidationResult
    final case object HookMissing extends HookValidationResult
  }

  final case class NoAccessTokenException(message: String) extends RuntimeException(message)

  def apply(
      projectHookUrl:  ProjectHookUrl,
      gitLabThrottler: Throttler[IO, GitLab]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[HookValidator[IO]] = for {
    tokenRepositoryUrl <- TokenRepositoryUrl[IO]()
    gitLabUrl          <- GitLabUrl[IO]()
  } yield new HookValidatorImpl[IO](
    projectHookUrl,
    new IOProjectHookVerifier(gitLabUrl, gitLabThrottler, ApplicationLogger),
    new IOAccessTokenFinder(tokenRepositoryUrl, ApplicationLogger),
    new IOAccessTokenAssociator(tokenRepositoryUrl, ApplicationLogger),
    new IOAccessTokenRemover(tokenRepositoryUrl, ApplicationLogger),
    ApplicationLogger
  )
}
