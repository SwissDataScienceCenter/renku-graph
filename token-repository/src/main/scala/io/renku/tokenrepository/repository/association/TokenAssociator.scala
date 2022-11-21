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
import cats.effect.Async
import cats.syntax.all._
import deletion.{TokenRemover, TokenRemoverImpl}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import fetching.{PersistedTokensFinder, PersistedTokensFinderImpl}
import io.renku.graph.model.projects.{Id, Path}
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait TokenAssociator[F[_]] {
  def associate(projectId: Id, token: AccessToken): F[Unit]
}

private class TokenAssociatorImpl[F[_]: MonadThrow](
    projectPathFinder:         ProjectPathFinder[F],
    accessTokenCrypto:         AccessTokenCrypto[F],
    tokenValidator:            TokenValidator[F],
    projectAccessTokenCreator: ProjectAccessTokenCreator[F],
    associationPersister:      AssociationPersister[F],
    tokenRemover:              TokenRemover[F],
    tokenFinder:               PersistedTokensFinder[F],
    maxRetries:                Int Refined NonNegative
) extends TokenAssociator[F] {

  import accessTokenCrypto._
  import associationPersister._
  import projectAccessTokenCreator._
  import projectPathFinder._
  import tokenFinder._
  import tokenValidator._

  override def associate(projectId: Id, token: AccessToken): F[Unit] =
    findStoredToken(projectId).cataF(false.pure[F], validate) >>= {
      case true  => ().pure[F]
      case false => createOrDelete(projectId, token)
    }

  private def validate(encryptedToken: EncryptedAccessToken): F[Boolean] =
    decrypt(encryptedToken) >>= {
      case token: ProjectAccessToken => checkValid(token)
      case _ => false.pure[F]
    }

  private def createOrDelete(projectId: Id, token: AccessToken) =
    findProjectPath(projectId, token)
      .cataF(tokenRemover delete projectId, generateNewToken(projectId, _, token))

  private def generateNewToken(projectId: Id, projectPath: Path, token: AccessToken): F[Unit] =
    for {
      newProjectToken       <- createPersonalAccessToken(projectId, token)
      encryptedProjectToken <- encrypt(newProjectToken)
      _                     <- persistOrRetry(projectId, projectPath, newProjectToken, encryptedProjectToken)
    } yield ()

  private def persistOrRetry(projectId:       Id,
                             projectPath:     Path,
                             token:           ProjectAccessToken,
                             encryptedToken:  AccessTokenCrypto.EncryptedAccessToken,
                             numberOfRetries: Int = 0
  ): F[Unit] =
    persistAssociation(projectId, projectPath, encryptedToken) >>
      verifyTokenIntegrity(projectId, token)
        .recoverWith(retry(projectId, projectPath, token, encryptedToken, numberOfRetries))

  private def verifyTokenIntegrity(projectId: Id, token: ProjectAccessToken) =
    findStoredToken(projectId)
      .cataF(
        new Exception(show"Token associator - just saved token cannot be found for project: $projectId")
          .raiseError[F, Unit],
        decrypt(_) >>= {
          case `token` => ().pure[F]
          case _ =>
            new Exception(show"Token associator - just saved token integrity check failed for project: $projectId")
              .raiseError[F, Unit]
        }
      )

  private def retry(projectId:       Id,
                    projectPath:     Path,
                    token:           ProjectAccessToken,
                    encryptedToken:  AccessTokenCrypto.EncryptedAccessToken,
                    numberOfRetries: Int
  ): PartialFunction[Throwable, F[Unit]] = { case NonFatal(error) =>
    if (numberOfRetries < maxRetries.value) {
      persistOrRetry(projectId, projectPath, token, encryptedToken, numberOfRetries + 1)
    } else error.raiseError[F, Unit]
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
    projectAccessTokenCreator <- ProjectAccessTokenCreator[F]()
    persister    = new AssociationPersisterImpl(queriesExecTimes)
    tokenRemover = new TokenRemoverImpl[F](queriesExecTimes)
    tokenFinder  = new PersistedTokensFinderImpl[F](queriesExecTimes)
  } yield new TokenAssociatorImpl[F](pathFinder,
                                     accessTokenCrypto,
                                     tokenValidator,
                                     projectAccessTokenCreator,
                                     persister,
                                     tokenRemover,
                                     tokenFinder,
                                     maxRetries
  )
}
