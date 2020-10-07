/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.Monad
import cats.effect.{Bracket, ContextShift, IO}
import ch.datascience.db.DbTransactor
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.ProjectsTokensDB

private class AssociationPersister[Interpretation[_]: Monad](
    transactor: DbTransactor[Interpretation, ProjectsTokensDB]
)(implicit ME:  Bracket[Interpretation, Throwable]) {

  import doobie.implicits._

  def persistAssociation(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken): Interpretation[Unit] =
    sql"select token from projects_tokens where project_id = ${projectId.value}"
      .query[String]
      .option
      .flatMap {
        case Some(_) => update(projectId, projectPath, encryptedToken)
        case None    => insert(projectId, projectPath, encryptedToken)
      }
      .transact(transactor.get)

  private def insert(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken) =
    sql"""insert into 
          projects_tokens (project_id, project_path, token) 
          values (${projectId.value}, ${projectPath.value}, ${encryptedToken.value})
      """.update.run.map(failIfMultiUpdate(projectId, projectPath))

  private def update(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken) =
    sql"""update projects_tokens 
          set token = ${encryptedToken.value}, project_path = ${projectPath.value}  
          where project_id = ${projectId.value}
      """.update.run.map(failIfMultiUpdate(projectId, projectPath))

  private def failIfMultiUpdate(projectId: Id, projectPath: Path): Int => Unit = {
    case 1 => ()
    case _ => throw new RuntimeException(s"Associating token for project $projectPath ($projectId)")
  }
}

private class IOAssociationPersister(
    transactor:          DbTransactor[IO, ProjectsTokensDB]
)(implicit contextShift: ContextShift[IO])
    extends AssociationPersister[IO](transactor)
