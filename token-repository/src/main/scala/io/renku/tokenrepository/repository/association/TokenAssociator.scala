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

package io.renku.tokenrepository.repository.association

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.types.numeric
import io.renku.db.SessionResource
import io.renku.graph.model.projects.{Id, Path}
import io.renku.http.client.AccessToken
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.deletion.{TokenRemover, TokenRemoverImpl}
import io.renku.tokenrepository.repository.fetching.{PersistedTokensFinder, PersistedTokensFinderImpl}
import io.renku.tokenrepository.repository.{AccessTokenCrypto, ProjectsTokensDB}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait TokenAssociator[F[_]] {
  def associate(projectId: Id, token: AccessToken): F[Unit]
}

private class TokenAssociatorImpl[F[_]: MonadThrow](
    projectPathFinder:    ProjectPathFinder[F],
    accessTokenCrypto:    AccessTokenCrypto[F],
    associationPersister: AssociationPersister[F],
    tokenRemover:         TokenRemover[F],
    tokenFinder:          PersistedTokensFinder[F],
    maxRetries:           numeric.NonNegInt
) extends TokenAssociator[F] {

  import accessTokenCrypto._
  import associationPersister._
  import projectPathFinder._

  override def associate(projectId: Id, token: AccessToken): F[Unit] =
    findProjectPath(projectId, Some(token)) flatMap {
      case Some(projectPath) => encryptAndPersist(projectId, projectPath, token)
      case None              => tokenRemover delete projectId
    }

  private def encryptAndPersist(projectId: Id, projectPath: Path, token: AccessToken) = for {
    encryptedToken <- encrypt(token)
    _              <- persistOrRetry(projectId, projectPath, encryptedToken)
  } yield ()

  private def persistOrRetry(projectId:       Id,
                             projectPath:     Path,
                             encryptedToken:  AccessTokenCrypto.EncryptedAccessToken,
                             numberOfRetries: Int = 0
  ): F[Unit] = for {
    _ <- persistAssociation(projectId, projectPath, encryptedToken)
    _ <- verifyTokenIntegrity(projectPath) recoverWith retry(projectId, projectPath, encryptedToken, numberOfRetries)
  } yield ()

  private def verifyTokenIntegrity(projectPath: Path) =
    tokenFinder.findToken(projectPath).value.flatMap {
      case Some(savedToken) => decrypt(savedToken).void
      case _ =>
        new Exception(show"Token associator - saved encrypted token cannot be found for project: $projectPath")
          .raiseError[F, Unit]
    }

  private def retry(projectId:       Id,
                    projectPath:     Path,
                    encryptedToken:  AccessTokenCrypto.EncryptedAccessToken,
                    numberOfRetries: Int
  ): PartialFunction[Throwable, F[Unit]] = { case NonFatal(error) =>
    if (numberOfRetries < maxRetries.value) {
      persistOrRetry(projectId, projectPath, encryptedToken, numberOfRetries + 1)
    } else error.raiseError[F, Unit]
  }
}

private object TokenAssociator {

  private val maxRetries: numeric.NonNegInt = numeric.NonNegInt.unsafeFrom(3)

  def apply[F[_]: Async: Logger](
      sessionResource:  SessionResource[F, ProjectsTokensDB],
      queriesExecTimes: LabeledHistogram[F]
  ): F[TokenAssociator[F]] = for {
    pathFinder        <- ProjectPathFinder[F]
    accessTokenCrypto <- AccessTokenCrypto[F]()
    persister    = new AssociationPersisterImpl(sessionResource, queriesExecTimes)
    tokenRemover = new TokenRemoverImpl[F](sessionResource, queriesExecTimes)
    tokenFinder  = new PersistedTokensFinderImpl[F](sessionResource, queriesExecTimes)
  } yield new TokenAssociatorImpl[F](pathFinder, accessTokenCrypto, persister, tokenRemover, tokenFinder, maxRetries)
}
