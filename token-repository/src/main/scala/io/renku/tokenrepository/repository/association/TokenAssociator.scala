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

package io.renku.tokenrepository.repository
package association

import AccessTokenCrypto.EncryptedAccessToken
import ProjectsTokensDB.SessionResource
import cats.MonadThrow
import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import deletion.TokenRemover
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import fetching.PersistedTokensFinder
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait TokenAssociator[F[_]] {
  def associate(projectId: projects.Id, token: AccessToken): F[Unit]
}

private class TokenAssociatorImpl[F[_]: MonadThrow: Logger](
    projectPathFinder:    ProjectPathFinder[F],
    accessTokenCrypto:    AccessTokenCrypto[F],
    tokenValidator:       TokenValidator[F],
    tokenDueChecker:      TokenDueChecker[F],
    tokensCreator:        TokensCreator[F],
    associationPersister: AssociationPersister[F],
    persistedPathFinder:  PersistedPathFinder[F],
    tokenRemover:         TokenRemover[F],
    tokenFinder:          PersistedTokensFinder[F],
    maxRetries:           Int Refined NonNegative
) extends TokenAssociator[F] {

  import associationPersister._
  import persistedPathFinder._
  import projectPathFinder._
  import tokenDueChecker._
  import tokenFinder._
  import tokenValidator._
  import tokensCreator._

  override def associate(projectId: projects.Id, token: AccessToken): F[Unit] =
    findStoredToken(projectId)
      .flatMapF(decrypt >=> validate >=> checkIfDue(projectId))
      .semiflatMap(replacePathIfChanged(projectId))
      .getOrElseF(createOrDelete(projectId, token))

  private lazy val decrypt: EncryptedAccessToken => F[Option[ProjectAccessToken]] = encryptedToken =>
    accessTokenCrypto.decrypt(encryptedToken) map {
      case token: ProjectAccessToken => token.some
      case _ => Option.empty[ProjectAccessToken]
    }

  private lazy val validate: Option[ProjectAccessToken] => F[Option[ProjectAccessToken]] = {
    case Some(token) => checkValid(token).map(Option.when(_)(token))
    case _           => Option.empty[ProjectAccessToken].pure[F]
  }

  private def checkIfDue(projectId: projects.Id): Option[ProjectAccessToken] => F[Option[ProjectAccessToken]] = {
    case Some(token) => checkTokenDue(projectId).map(Option.unless(_)(token))
    case _           => Option.empty[ProjectAccessToken].pure[F]
  }

  private def replacePathIfChanged(projectId: projects.Id)(token: ProjectAccessToken): F[Unit] =
    findProjectPath(projectId, token)
      .semiflatMap(actualPath =>
        findPersistedProjectPath(projectId).flatMap {
          case `actualPath` => ().pure[F]
          case _            => updatePath(Project(projectId, actualPath))
        }
      )
      .getOrElseF(removeToken(projectId))

  private def createOrDelete(projectId: projects.Id, token: AccessToken) =
    findProjectPath(projectId, token)
      .map(Project(projectId, _))
      .flatMap(maybeGenerateNewToken(_, token))
      .getOrElseF(removeToken(projectId))

  private def removeToken(projectId: projects.Id) =
    Logger[F].info(show"removing token as no project path found for project $projectId") >>
      tokenRemover.delete(projectId)

  private def maybeGenerateNewToken(project: Project, token: AccessToken) =
    OptionT(createPersonalAccessToken(project.id, token))
      .semiflatMap { newTokenInfo =>
        accessTokenCrypto.encrypt(newTokenInfo.token) >>=
          (encToken => persistOrRetry(TokenStoringInfo(project, encToken, newTokenInfo.dates), newTokenInfo.token))
      }

  private def persistOrRetry(storingInfo:     TokenStoringInfo,
                             newToken:        ProjectAccessToken,
                             numberOfRetries: Int = 0
  ): F[Unit] =
    persistAssociation(storingInfo) >>
      verifyTokenIntegrity(storingInfo.project.id, newToken)
        .recoverWith(retry(storingInfo, newToken, numberOfRetries))

  private def verifyTokenIntegrity(projectId: projects.Id, token: ProjectAccessToken) =
    findStoredToken(projectId)
      .cataF(
        new Exception(show"Token associator - just saved token cannot be found for project: $projectId")
          .raiseError[F, Unit],
        accessTokenCrypto.decrypt(_) >>= {
          case `token` => ().pure[F]
          case _ =>
            new Exception(show"Token associator - just saved token integrity check failed for project: $projectId")
              .raiseError[F, Unit]
        }
      )

  private def retry(storingInfo:     TokenStoringInfo,
                    newToken:        ProjectAccessToken,
                    numberOfRetries: Int
  ): PartialFunction[Throwable, F[Unit]] = { case NonFatal(error) =>
    if (numberOfRetries >= maxRetries.value) error.raiseError[F, Unit]
    else persistOrRetry(storingInfo, newToken, numberOfRetries + 1)
  }
}

private object TokenAssociator {

  private val maxRetries: Int Refined NonNegative = 3

  def apply[F[_]: Async: GitLabClient: Logger: SessionResource](
      queriesExecTimes: LabeledHistogram[F]
  ): F[TokenAssociator[F]] = for {
    pathFinder                <- ProjectPathFinder[F]
    accessTokenCrypto         <- AccessTokenCrypto[F]()
    tokenValidator            <- TokenValidator[F]
    tokenDueChecker           <- TokenDueChecker[F](queriesExecTimes)
    projectAccessTokenCreator <- TokensCreator[F]()
  } yield new TokenAssociatorImpl[F](
    pathFinder,
    accessTokenCrypto,
    tokenValidator,
    tokenDueChecker,
    projectAccessTokenCreator,
    AssociationPersister(queriesExecTimes),
    PersistedPathFinder(queriesExecTimes),
    TokenRemover(queriesExecTimes),
    PersistedTokensFinder(queriesExecTimes),
    maxRetries
  )
}
