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

package ch.datascience.tokenrepository.repository.association

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.db.{SessionResource, SqlQuery}
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.http.client.AccessToken
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.deletion.TokenRemover
import ch.datascience.tokenrepository.repository.{AccessTokenCrypto, ProjectsTokensDB}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private class TokenAssociator[Interpretiation[_]](
    projectPathFinder:    ProjectPathFinder[Interpretiation],
    accessTokenCrypto:    AccessTokenCrypto[Interpretiation],
    associationPersister: AssociationPersister[Interpretiation],
    tokenRemover:         TokenRemover[Interpretiation]
)(implicit ME:            MonadError[Interpretiation, Throwable]) {

  import accessTokenCrypto._
  import associationPersister._
  import projectPathFinder._

  def associate(projectId: Id, token: AccessToken): Interpretiation[Unit] =
    findProjectPath(projectId, Some(token)) flatMap {
      case Some(projectPath) => encryptAndPersist(projectId, projectPath, token)
      case None              => tokenRemover delete projectId
    }

  private def encryptAndPersist(projectId: Id, projectPath: Path, token: AccessToken) =
    for {
      encryptedToken <- encrypt(token)
      _              <- persistAssociation(projectId, projectPath, encryptedToken)
    } yield ()
}

private object IOTokenAssociator {
  def apply(
      sessionResource:  SessionResource[IO, ProjectsTokensDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
      logger:           Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[TokenAssociator[IO]] =
    for {
      pathFinder        <- IOProjectPathFinder(logger)
      accessTokenCrypto <- AccessTokenCrypto[IO]()
      persister    = new IOAssociationPersister(sessionResource, queriesExecTimes)
      tokenRemover = new TokenRemover[IO](sessionResource, queriesExecTimes)
    } yield new TokenAssociator[IO](pathFinder, accessTokenCrypto, persister, tokenRemover)
}
