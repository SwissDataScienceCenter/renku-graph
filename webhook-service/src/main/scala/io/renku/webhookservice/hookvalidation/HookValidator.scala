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

import cats.MonadThrow
import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.auto._
import io.renku.cache.{Cache, CacheBuilder, CacheConfigLoader}
import io.renku.graph.model.projects.GitLabId
import io.renku.graph.tokenrepository.{AccessTokenFinder, AccessTokenFinderImpl, TokenRepositoryUrl}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.metrics.MetricsRegistry
import io.renku.webhookservice.hookvalidation.HookValidator.HookValidationResult
import io.renku.webhookservice.model.{HookIdentifier, ProjectHookUrl}
import io.renku.webhookservice.tokenrepository._
import org.typelevel.log4cats.Logger

trait HookValidator[F[_]] {
  def validateHook(projectId: GitLabId, maybeAccessToken: Option[AccessToken]): F[Option[HookValidationResult]]
}

object HookValidator {

  sealed trait HookValidationResult
  object HookValidationResult {
    final case object HookExists  extends HookValidationResult
    final case object HookMissing extends HookValidationResult
  }

  def apply[F[_]: Async: GitLabClient: Logger: MetricsRegistry](projectHookUrl: ProjectHookUrl): F[HookValidator[F]] =
    for {
      tokenRepositoryUrl    <- TokenRepositoryUrl[F]()
      projectHookVerifier   <- ProjectHookVerifier[F]
      accessTokenAssociator <- AccessTokenAssociator[F]
      cacheConfig           <- CacheConfigLoader.load[F]("services.self.hook-validation-cache", ConfigFactory.load())
      validationCache <- CacheBuilder
                           .default[F, GitLabId, HookValidationResult]
                           .withConfig(cacheConfig)
                           .withCacheStats("hook_validator_cache")
                           .build
    } yield new HookValidatorImpl[F](
      projectHookUrl,
      projectHookVerifier,
      new AccessTokenFinderImpl(tokenRepositoryUrl),
      accessTokenAssociator,
      validationCache
    )
}

private class HookValidatorImpl[F[_]: MonadThrow: Logger](
    projectHookUrl:        ProjectHookUrl,
    projectHookVerifier:   ProjectHookVerifier[F],
    accessTokenFinder:     AccessTokenFinder[F],
    accessTokenAssociator: AccessTokenAssociator[F],
    validationCache:       Cache[F, GitLabId, HookValidationResult]
) extends HookValidator[F] {

  import HookValidator.HookValidationResult._
  import HookValidator._
  import accessTokenAssociator._
  import accessTokenFinder._
  import projectHookVerifier._

  def validateHook(projectId: GitLabId, maybeAccessToken: Option[AccessToken]): F[Option[HookValidationResult]] =
    validationCache.withCache(pId => validateHook0(pId, maybeAccessToken))(projectId)

  private def validateHook0(projectId: GitLabId, mat: Option[AccessToken]): F[Option[HookValidationResult]] = {
    persistGivenToken(projectId, mat) >> validateHookExistence(projectId)
  }.onError(logError(projectId))

  private def persistGivenToken(projectId: GitLabId, maybeAccessToken: Option[AccessToken]): F[Unit] =
    maybeAccessToken
      .map(associate(projectId, _))
      .getOrElse(().pure[F])

  private def validateHookExistence(projectId: GitLabId): F[Option[HookValidationResult]] =
    fetchToken(projectId)
      .flatMapF(checkHookPresence(HookIdentifier(projectId, projectHookUrl), _))
      .cataF(
        default = Option.empty[HookValidationResult].pure[F],
        toValidationResult(_).some.pure[F]
      )

  private def fetchToken(projectId: GitLabId): OptionT[F, AccessToken] = OptionT {
    findAccessToken(projectId)
      .adaptError(storedAccessTokenError(projectId))
  }

  private def storedAccessTokenError(projectId: GitLabId): PartialFunction[Throwable, Throwable] =
    new Exception(s"Finding stored access token for $projectId failed", _)

  private def toValidationResult(hookPresent: Boolean): HookValidationResult =
    if (hookPresent) HookExists else HookMissing

  private def logError(projectId: GitLabId): PartialFunction[Throwable, F[Unit]] = { case exception =>
    Logger[F].error(exception)(s"Hook validation failed for projectId $projectId")
  }
}
