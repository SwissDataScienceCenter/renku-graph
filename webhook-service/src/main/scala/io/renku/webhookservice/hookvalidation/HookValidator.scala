/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.{Applicative, MonadThrow}
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.projects.GitLabId
import io.renku.graph.tokenrepository.{AccessTokenFinder, AccessTokenFinderImpl, TokenRepositoryUrl}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult
import io.renku.webhookservice.model.{HookIdentifier, ProjectHookUrl}
import io.renku.webhookservice.tokenrepository._
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait HookValidator[F[_]] {
  def validateHook(projectId: GitLabId, maybeAccessToken: Option[AccessToken]): F[HookValidationResult]
}

class HookValidatorImpl[F[_]: MonadThrow: Logger](
    projectHookUrl:        ProjectHookUrl,
    projectHookVerifier:   ProjectHookVerifier[F],
    accessTokenFinder:     AccessTokenFinder[F],
    accessTokenAssociator: AccessTokenAssociator[F],
    accessTokenRemover:    AccessTokenRemover[F]
) extends HookValidator[F] {

  private val applicative = Applicative[F]

  import HookValidator._
  import HookValidator.HookValidationResult._
  import Token._
  import accessTokenAssociator._
  import accessTokenFinder._
  import accessTokenRemover._
  import applicative._
  import projectHookVerifier._

  def validateHook(projectId: GitLabId, maybeAccessToken: Option[AccessToken]): F[HookValidationResult] =
    findToken(projectId, maybeAccessToken)
      .flatMap {
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
      }
      .onError(logError(projectId))

  private def findToken(projectId: GitLabId, maybeAccessToken: Option[AccessToken]): F[Token] =
    maybeAccessToken
      .map(GivenToken(_).widen.pure[F])
      .getOrElse(findStoredToken(projectId))

  private def findStoredToken(projectId: GitLabId): F[Token] =
    findAccessToken(projectId)
      .adaptError(storedAccessTokenError(projectId))
      .flatMap(getOrError(projectId))

  private def storedAccessTokenError(projectId: GitLabId): PartialFunction[Throwable, Throwable] = exception =>
    new Exception(s"Finding stored access token for $projectId failed", exception)

  private def getOrError(projectId: GitLabId): Option[AccessToken] => F[Token] = {
    case Some(token) =>
      StoredToken(token).widen.pure[F]
    case None =>
      NoAccessTokenException(s"No stored access token found for projectId $projectId").raiseError[F, Token]
  }

  private def toValidationResult(projectHookPresent: Boolean): HookValidationResult =
    if (projectHookPresent) HookExists else HookMissing

  private def logError(projectId: GitLabId): PartialFunction[Throwable, F[Unit]] = {
    case NoAccessTokenException(message) =>
      Logger[F].info(s"Hook validation failed: $message")
    case NonFatal(exception) =>
      Logger[F].error(exception)(s"Hook validation failed for projectId $projectId")
  }

  private sealed abstract class Token(val value: AccessToken) {
    lazy val widen: Token = this
  }
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

  def apply[F[_]: Async: GitLabClient: Logger](projectHookUrl: ProjectHookUrl): F[HookValidator[F]] = for {
    tokenRepositoryUrl    <- TokenRepositoryUrl[F]()
    projectHookVerifier   <- ProjectHookVerifier[F]
    accessTokenAssociator <- AccessTokenAssociator[F]
    accessTokenRemover    <- AccessTokenRemover[F]
  } yield new HookValidatorImpl[F](projectHookUrl,
                                   projectHookVerifier,
                                   new AccessTokenFinderImpl(tokenRepositoryUrl),
                                   accessTokenAssociator,
                                   accessTokenRemover
  )
}
