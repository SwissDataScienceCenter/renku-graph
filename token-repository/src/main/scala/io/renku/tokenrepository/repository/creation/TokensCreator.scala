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
package creation

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
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait TokensCreator[F[_]] {
  def create(projectId: projects.Id, token: AccessToken): F[Unit]
}

private class TokensCreatorImpl[F[_]: MonadThrow: Logger](
    projectPathFinder:      ProjectPathFinder[F],
    accessTokenCrypto:      AccessTokenCrypto[F],
    tokenValidator:         TokenValidator[F],
    tokenDueChecker:        TokenDueChecker[F],
    newTokensCreator:       NewTokensCreator[F],
    tokensPersister:        TokensPersister[F],
    persistedPathFinder:    PersistedPathFinder[F],
    tokenRemover:           TokenRemover[F],
    tokenFinder:            PersistedTokensFinder[F],
    revokeCandidatesFinder: RevokeCandidatesFinder[F],
    tokensRevoker:          TokensRevoker[F],
    maxRetries:             Int Refined NonNegative
) extends TokensCreator[F] {

  import newTokensCreator._
  import persistedPathFinder._
  import revokeCandidatesFinder._
  import tokenDueChecker._
  import tokenFinder._
  import tokenValidator._
  import tokensPersister._
  import tokensRevoker._

  override def create(projectId: projects.Id, userToken: AccessToken): F[Unit] =
    findStoredToken(projectId)
      .flatMapF(decrypt >=> validate >=> checkIfDue(projectId))
      .semiflatMap(replacePathIfChanged(projectId))
      .getOrElseF(createOrDelete(projectId, userToken))

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
    projectPathFinder
      .findProjectPath(projectId, token)
      .semiflatMap(actualPath =>
        findPersistedProjectPath(projectId).flatMap {
          case `actualPath` => ().pure[F]
          case _            => updatePath(Project(projectId, actualPath))
        }
      )
      .getOrElseF(tokenRemover.delete(projectId))

  private def createOrDelete(projectId: projects.Id, userToken: AccessToken) =
    (findProjectPath(projectId, userToken) >>= createNewToken(userToken))
      .semiflatMap(encrypt >=> persist >=> logSuccess >=> tryRevokingOldTokens(userToken))
      .getOrElseF(tokenRemover.delete(projectId))

  private def findProjectPath(projectId: projects.Id, userToken: AccessToken): OptionT[F, Project] =
    projectPathFinder.findProjectPath(projectId, userToken).map(Project(projectId, _))

  private def createNewToken(userToken: AccessToken)(project: Project): OptionT[F, (Project, TokenCreationInfo)] =
    createPersonalAccessToken(project.id, userToken).map(project -> _)

  private lazy val encrypt: ((Project, TokenCreationInfo)) => F[(Project, TokenCreationInfo, EncryptedAccessToken)] = {
    case (project, creationInfo) =>
      accessTokenCrypto.encrypt(creationInfo.token).map((project, creationInfo, _))
  }

  private lazy val persist: ((Project, TokenCreationInfo, EncryptedAccessToken)) => F[Project] = {
    case (project, creationInfo, encToken) =>
      persistWithRetry(TokenStoringInfo(project, encToken, creationInfo.dates), creationInfo.token).map(_ => project)
  }

  private def logSuccess(project: Project): F[Project] =
    Logger[F].info(show"token created for $project").map(_ => project)

  private def tryRevokingOldTokens(userToken: AccessToken)(project: Project) =
    findTokensToRemove(project.id, userToken)
      .flatMap(_.map(revokeToken(project.id, _, userToken)).sequence)
      .void
      .recoverWith { case NonFatal(ex) =>
        Logger[F].warn(ex)(show"removing old token in GitLab for project $project failed")
      }

  private def persistWithRetry(storingInfo:     TokenStoringInfo,
                               newToken:        ProjectAccessToken,
                               numberOfRetries: Int = 0
  ): F[Unit] =
    persistToken(storingInfo) >>
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
    else persistWithRetry(storingInfo, newToken, numberOfRetries + 1)
  }
}

private object TokensCreator {

  private val maxRetries: Int Refined NonNegative = 3

  def apply[F[_]: Async: GitLabClient: Logger: SessionResource: QueriesExecutionTimes]: F[TokensCreator[F]] = for {
    pathFinder                <- ProjectPathFinder[F]
    accessTokenCrypto         <- AccessTokenCrypto[F]()
    tokenValidator            <- TokenValidator[F]
    tokenDueChecker           <- TokenDueChecker[F]
    projectAccessTokenCreator <- NewTokensCreator[F]()
    revokeCandidatesFinder    <- RevokeCandidatesFinder[F]
  } yield new TokensCreatorImpl[F](
    pathFinder,
    accessTokenCrypto,
    tokenValidator,
    tokenDueChecker,
    projectAccessTokenCreator,
    TokensPersister[F],
    PersistedPathFinder[F],
    TokenRemover[F],
    PersistedTokensFinder[F],
    revokeCandidatesFinder,
    TokensRevoker[F],
    maxRetries
  )
}