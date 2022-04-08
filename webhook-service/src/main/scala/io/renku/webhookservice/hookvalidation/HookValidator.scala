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

package io.renku.webhookservice.hookvalidation

import cats.effect.Async
import cats.syntax.all._
import cats.{Applicative, MonadThrow}
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.model.projects.Id
import io.renku.graph.tokenrepository.{AccessTokenFinder, AccessTokenFinderImpl, TokenRepositoryUrl}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.client.RestClientError.UnauthorizedException
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult
import io.renku.webhookservice.model.{HookIdentifier, ProjectHookUrl}
import io.renku.webhookservice.tokenrepository._
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait HookValidator[F[_]] {
  def validateHook(projectId: Id, maybeAccessToken: Option[AccessToken]): F[HookValidationResult]
}

class HookValidatorImpl[F[_]: MonadThrow: Logger](
    projectHookUrl:        ProjectHookUrl,
    projectHookVerifier:   ProjectHookVerifier[F],
    accessTokenFinder:     AccessTokenFinder[F],
    accessTokenAssociator: AccessTokenAssociator[F],
    accessTokenRemover:    AccessTokenRemover[F]
) extends HookValidator[F] {

  private val applicative = Applicative[F]

  import AccessTokenFinder._
  import HookValidator.HookValidationResult._
  import HookValidator._
  import Token._
  import accessTokenAssociator._
  import accessTokenFinder._
  import accessTokenRemover._
  import applicative._
  import projectHookVerifier._

  def validateHook(projectId: Id, maybeAccessToken: Option[AccessToken]): F[HookValidationResult] =
    findToken(projectId, maybeAccessToken) flatMap {
      case GivenToken(token) =>
        for {
          hookPresent      <- checkHookPresence(HookIdentifier(projectId, projectHookUrl), token)
          _                <- whenA(hookPresent)(associate(projectId, token))
          _                <- whenA(!hookPresent)(removeAccessToken(projectId))
          validationResult <- toValidationResult(hookPresent).pure[F]
        } yield validationResult
      case StoredToken(token) =>
        for {
          hookPresent      <- checkHookPresence(HookIdentifier(projectId, projectHookUrl), token)
          _                <- whenA(!hookPresent)(removeAccessToken(projectId))
          validationResult <- toValidationResult(hookPresent).pure[F]
        } yield validationResult
    } recoverWith loggingError(projectId)

  private def findToken(
      projectId:        Id,
      maybeAccessToken: Option[AccessToken]
  ): F[Token] =
    maybeAccessToken
      .map(token => (GivenToken(token): Token).pure[F])
      .getOrElse(findStoredToken(projectId))

  private def findStoredToken(projectId: Id): F[Token] =
    findAccessToken(projectId) flatMap getOrError(projectId) recoverWith storedAccessTokenError(projectId)

  private def getOrError(projectId: Id): Option[AccessToken] => F[Token] = {
    case Some(token) =>
      (StoredToken(token): Token).pure[F]
    case None =>
      NoAccessTokenException(s"No access token found for projectId $projectId").raiseError[F, Token]
  }

  private def storedAccessTokenError(
      projectId: Id
  ): PartialFunction[Throwable, F[Token]] = { case UnauthorizedException =>
    new Exception(s"Stored access token for $projectId is invalid").raiseError[F, Token]
  }

  private def toValidationResult(projectHookPresent: Boolean): HookValidationResult =
    if (projectHookPresent) HookExists else HookMissing

  private def loggingError(projectId: Id): PartialFunction[Throwable, F[HookValidationResult]] = {
    case exception @ NoAccessTokenException(message) =>
      Logger[F].info(s"Hook validation failed: $message") >>
        exception.raiseError[F, HookValidationResult]
    case NonFatal(exception) =>
      Logger[F].error(exception)(s"Hook validation failed for project with id $projectId") >>
        exception.raiseError[F, HookValidationResult]
  }

  private sealed abstract class Token(val value: AccessToken)
  private object Token {
    case class GivenToken(override val value: AccessToken)  extends Token(value)
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

  def apply[F[_]: Async: Logger](
      projectHookUrl:  ProjectHookUrl,
      gitLabThrottler: Throttler[F, GitLab],
      gitLabClient:    GitLabClient[F]
  ): F[HookValidator[F]] = for {
    tokenRepositoryUrl    <- TokenRepositoryUrl[F]()
    projectHookVerifier   <- ProjectHookVerifier[F](gitLabThrottler, gitLabClient)
    accessTokenAssociator <- AccessTokenAssociator[F]
    accessTokenRemover    <- AccessTokenRemover[F]
  } yield new HookValidatorImpl[F](
    projectHookUrl,
    projectHookVerifier,
    new AccessTokenFinderImpl(tokenRepositoryUrl),
    accessTokenAssociator,
    accessTokenRemover
  )
}
