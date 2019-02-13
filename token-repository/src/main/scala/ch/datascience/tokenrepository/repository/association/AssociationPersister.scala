/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import cats.effect.{ContextShift, IO}
import cats.implicits._
import cats.{Monad, MonadError}
import ch.datascience.db.TransactorProvider
import ch.datascience.graph.events.ProjectId
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.ProjectsTokensConfig

import scala.language.higherKinds

private class AssociationPersister[Interpretation[_]: Monad](
    transactorProvider: TransactorProvider[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  import doobie.implicits._

  def persistAssociation(projectId: ProjectId, encryptedToken: EncryptedAccessToken): Interpretation[Unit] =
    for {
      transactor <- transactorProvider.transactor
      _ <- sql"select token from projects_tokens where project_id = ${projectId.value}"
            .query[String]
            .option
            .flatMap {
              case Some(_) => update(projectId, encryptedToken)
              case None    => insert(projectId, encryptedToken)
            }
            .transact(transactor)
    } yield ()

  private def insert(projectId: ProjectId, encryptedToken: EncryptedAccessToken) =
    sql"""insert into 
          projects_tokens (project_id, token) 
          values (${projectId.value}, ${encryptedToken.value})
      """.update.run.map(failIfMultiUpdate(projectId))

  private def update(projectId: ProjectId, encryptedToken: EncryptedAccessToken) =
    sql"""update projects_tokens 
          set token = ${encryptedToken.value} 
          where project_id = ${projectId.value}
      """.update.run.map(failIfMultiUpdate(projectId))

  private def failIfMultiUpdate(projectId: ProjectId): Int => Unit = {
    case 1 => ()
    case _ => throw new RuntimeException(s"Associating token for a projectId: $projectId")
  }
}

private class IOAssociationPersister(implicit contextShift: ContextShift[IO])
    extends AssociationPersister[IO](new TransactorProvider[IO](new ProjectsTokensConfig[IO]))
