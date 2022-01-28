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
import cats.effect.kernel.Temporal
import cats.syntax.all._
import io.renku.db.{SessionResource, SqlStatement}
import io.renku.graph.model.projects.{Id, Path}
import io.renku.http.client.AccessToken
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.deletion.{TokenRemover, TokenRemoverImpl}
import io.renku.tokenrepository.repository.{AccessTokenCrypto, ProjectsTokensDB}
import org.typelevel.log4cats.Logger

private trait TokenAssociator[F[_]] {
  def associate(projectId: Id, token: AccessToken): F[Unit]
}

private class TokenAssociatorImpl[F[_]: MonadThrow](
    projectPathFinder:    ProjectPathFinder[F],
    accessTokenCrypto:    AccessTokenCrypto[F],
    associationPersister: AssociationPersister[F],
    tokenRemover:         TokenRemover[F]
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
    _              <- persistAssociation(projectId, projectPath, encryptedToken)
  } yield ()
}

private object TokenAssociator {
  def apply[F[_]: Async: Temporal: Logger](
      sessionResource:  SessionResource[F, ProjectsTokensDB],
      queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[TokenAssociator[F]] = for {
    pathFinder        <- ProjectPathFinder[F]
    accessTokenCrypto <- AccessTokenCrypto[F]()
    persister    = new AssociationPersisterImpl(sessionResource, queriesExecTimes)
    tokenRemover = new TokenRemoverImpl[F](sessionResource, queriesExecTimes)
  } yield new TokenAssociatorImpl[F](pathFinder, accessTokenCrypto, persister, tokenRemover)
}
