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

package io.renku.tokenrepository.repository
package creation

import cats.MonadThrow
import cats.data.OptionT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import io.renku.graph.model.projects
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import io.renku.tokenrepository.repository.deletion.{DeletionResult, TokenRemover, TokensRevoker}
import io.renku.tokenrepository.repository.fetching.PersistedTokensFinder
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait TokensCreator[F[_]] {
  def create(projectId: projects.GitLabId, token: AccessToken): F[Unit]
}

private class TokensCreatorImpl[F[_]: MonadThrow: Logger](
    projectSlugFinder:   ProjectSlugFinder[F],
    accessTokenCrypto:   AccessTokenCrypto[F],
    tokenValidator:      TokenValidator[F],
    tokenDueChecker:     TokenDueChecker[F],
    newTokensCreator:    NewTokensCreator[F],
    tokensPersister:     TokensPersister[F],
    persistedSlugFinder: PersistedSlugFinder[F],
    tokenRemover:        TokenRemover[F],
    tokenFinder:         PersistedTokensFinder[F],
    tokensRevoker:       TokensRevoker[F],
    maxRetries:          Int Refined NonNegative
) extends TokensCreator[F] {

  import newTokensCreator._
  import persistedSlugFinder._
  import tokenDueChecker._
  import tokenFinder._
  import tokenValidator._
  import tokensPersister._

  override def create(projectId: projects.GitLabId, userToken: AccessToken): F[Unit] =
    findStoredToken(projectId)
      .flatMapF(
        decrypt >=>
          removeWhenInvalid(projectId, userToken) >=>
          replaceSlugIfChangedOrRemove(projectId, userToken) >=>
          checkIfDue(projectId)
      )
      .void
      .getOrElseF(createNew(projectId, userToken))

  private lazy val decrypt: EncryptedAccessToken => F[Option[AccessToken]] = encryptedToken =>
    accessTokenCrypto.decrypt(encryptedToken).map(_.some)

  private def removeWhenInvalid(projectId: projects.GitLabId,
                                userToken: AccessToken
  ): Option[AccessToken] => F[Option[AccessToken]] = {
    case None => Option.empty[AccessToken].pure[F]
    case Some(storedToken) =>
      checkValid(projectId, storedToken) >>= {
        case true => storedToken.some.pure[F]
        case false =>
          deleteAndLogSuccess(projectId, userToken, show"Token removed for $projectId as got invalidated in GL")
            .as(Option.empty)
      }
  }

  private def deleteAndLogSuccess(projectId: projects.GitLabId, mat: AccessToken, message: String) =
    tokenRemover.delete(projectId, mat.some) >>= {
      case DeletionResult.Deleted    => Logger[F].info(message)
      case DeletionResult.NotExisted => ().pure[F]
    }

  private def replaceSlugIfChangedOrRemove(
      projectId: projects.GitLabId,
      userToken: AccessToken
  ): Option[AccessToken] => F[Option[AccessToken]] = {
    case None => Option.empty[AccessToken].pure[F]
    case Some(storeToken) =>
      projectSlugFinder
        .findProjectSlug(projectId, storeToken)
        .flatMap {
          case None =>
            deleteAndLogSuccess(projectId,
                                userToken,
                                show"Token removed for $projectId as project does not exist in GL"
            ).as(Option.empty)
          case Some(actualGLSlug) =>
            findPersistedProjectSlug(projectId) >>= {
              case Some(`actualGLSlug`) => storeToken.some.pure[F]
              case Some(_)              => updateSlug(Project(projectId, actualGLSlug)).as(storeToken.some)
              case _                    => Option.empty[AccessToken].pure[F]
            }
        }
  }

  private def checkIfDue(projectId: projects.GitLabId): Option[AccessToken] => F[Option[AccessToken]] = {
    case Some(storedToken) => checkTokenDue(projectId).map(Option.unless(_)(storedToken))
    case _                 => Option.empty[AccessToken].pure[F]
  }

  private def createNew(projectId: projects.GitLabId, userToken: AccessToken) =
    tokenValidator.checkValid(projectId, userToken) >>= {
      case false => ().pure[F]
      case true =>
        (findProjectSlug(projectId, userToken) >>= createNewToken(userToken))
          .semiflatMap(encrypt >=> persist >=> logSuccess >=> tryRevokingOldTokens(userToken))
          .getOrElse(())
    }

  private def findProjectSlug(projectId: projects.GitLabId, userToken: AccessToken): OptionT[F, Project] =
    OptionT(projectSlugFinder.findProjectSlug(projectId, userToken)).map(Project(projectId, _))

  private def createNewToken(userToken: AccessToken)(project: Project): OptionT[F, (Project, TokenCreationInfo)] =
    createProjectAccessToken(project.id, userToken).map(project -> _)

  private lazy val encrypt: ((Project, TokenCreationInfo)) => F[(Project, TokenCreationInfo, EncryptedAccessToken)] = {
    case (project, creationInfo) =>
      accessTokenCrypto.encrypt(creationInfo.token).map((project, creationInfo, _))
  }

  private lazy val persist: ((Project, TokenCreationInfo, EncryptedAccessToken)) => F[(Project, TokenCreationInfo)] = {
    case (project, creationInfo, encToken) =>
      persistWithRetry(TokenStoringInfo(project, encToken, creationInfo.dates), creationInfo.token)
        .as(project -> creationInfo)
  }

  private lazy val logSuccess: ((Project, TokenCreationInfo)) => F[(Project, TokenCreationInfo)] = {
    case (project, creationInfo) =>
      Logger[F].info(show"token created for $project").as(project -> creationInfo)
  }

  private def tryRevokingOldTokens(userToken: AccessToken): ((Project, TokenCreationInfo)) => F[Unit] = {
    case (project, creationInfo) =>
      tokensRevoker.revokeAllTokens(project.id, creationInfo.tokenId.some, userToken)
  }

  private def persistWithRetry(storingInfo:     TokenStoringInfo,
                               newToken:        ProjectAccessToken,
                               numberOfRetries: Int = 0
  ): F[Unit] =
    persistToken(storingInfo) >>
      verifyTokenIntegrity(storingInfo.project.id, newToken)
        .recoverWith(retry(storingInfo, newToken, numberOfRetries))

  private def verifyTokenIntegrity(projectId: projects.GitLabId, token: ProjectAccessToken) =
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
    slugFinder                <- ProjectSlugFinder[F]
    accessTokenCrypto         <- AccessTokenCrypto[F]()
    tokenValidator            <- TokenValidator[F]
    tokenDueChecker           <- TokenDueChecker[F]
    projectAccessTokenCreator <- NewTokensCreator[F]()
    tokenRemover              <- TokenRemover[F]
    tokensRevoker             <- TokensRevoker[F]
  } yield new TokensCreatorImpl[F](
    slugFinder,
    accessTokenCrypto,
    tokenValidator,
    tokenDueChecker,
    projectAccessTokenCreator,
    TokensPersister[F],
    PersistedSlugFinder[F],
    tokenRemover,
    PersistedTokensFinder[F],
    tokensRevoker,
    maxRetries
  )
}
