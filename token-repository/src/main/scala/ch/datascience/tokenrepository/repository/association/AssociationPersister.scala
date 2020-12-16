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
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.ProjectsTokensDB
import eu.timepit.refined.auto._

private class AssociationPersister[Interpretation[_]: Monad](
    transactor:       DbTransactor[Interpretation, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
)(implicit ME:        Bracket[Interpretation, Throwable])
    extends DbClient(Some(queriesExecTimes)) {

  import doobie.implicits._

  def persistAssociation(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken): Interpretation[Unit] =
    upsert(projectId, projectPath, encryptedToken) transact transactor.get

  private def upsert(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken) =
    checkIfTokenExists(projectPath) flatMap {
      case true  => update(projectId, projectPath, encryptedToken)
      case false => insert(projectId, projectPath, encryptedToken)
    }

  private def checkIfTokenExists(projectPath: Path) = measureExecutionTime {
    SqlQuery(
      sql"SELECT token FROM projects_tokens WHERE project_path = ${projectPath.value}"
        .query[String]
        .option
        .map(_.isDefined),
      name = "associate token - check"
    )
  }

  private def update(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken) = measureExecutionTime {
    SqlQuery(
      sql"""UPDATE projects_tokens 
          SET token = ${encryptedToken.value}, project_id = ${projectId.value}
          WHERE project_path = ${projectPath.value}
      """.update.run.map(failIfMultiUpdate(projectId, projectPath)),
      name = "associate token - update"
    )
  }

  private def insert(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken) = measureExecutionTime {
    SqlQuery(
      sql"""INSERT INTO 
          projects_tokens (project_id, project_path, token) 
          VALUES (${projectId.value}, ${projectPath.value}, ${encryptedToken.value})
      """.update.run.map(failIfMultiUpdate(projectId, projectPath)),
      name = "associate token - insert"
    )
  }

  private def failIfMultiUpdate(projectId: Id, projectPath: Path): Int => Unit = {
    case 1 => ()
    case _ => throw new RuntimeException(s"Associating token for project $projectPath ($projectId)")
  }
}

private class IOAssociationPersister(
    transactor:          DbTransactor[IO, ProjectsTokensDB],
    queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
)(implicit contextShift: ContextShift[IO])
    extends AssociationPersister[IO](transactor, queriesExecTimes)
