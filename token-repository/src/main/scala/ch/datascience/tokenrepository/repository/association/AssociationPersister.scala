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

import cats.Monad
import cats.data.Kleisli
import cats.syntax.all._
import cats.effect._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.ProjectsTokensDB
import eu.timepit.refined.auto._
import skunk._
import skunk.codec.all._
import skunk.data.Completion
import skunk.data.Completion.Update
import skunk.implicits._

private class AssociationPersister[Interpretation[_]: Async: Monad](
    transactor:       SessionResource[Interpretation, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name]
)(implicit ME:        Bracket[Interpretation, Throwable])
    extends DbClient[Interpretation](Some(queriesExecTimes)) {

  def persistAssociation(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken): Interpretation[Unit] =
    transactor.use { session =>
      session.transaction.use { xa =>
        for {
          sp <- xa.savepoint
          _ <- upsert(projectId, projectPath, encryptedToken)(session).recoverWith { case e =>
                 xa.rollback(sp).flatMap(_ => e.raiseError[Interpretation, Unit])
               }
        } yield ()

      }
    }

  private def upsert(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken)(
      session:                  Session[Interpretation]
  ) =
    checkIfTokenExists(projectPath)(session) flatMap {
      case true  => update(projectId, projectPath, encryptedToken)(session)
      case false => insert(projectId, projectPath, encryptedToken)(session)
    }

  private def checkIfTokenExists(projectPath: Path)(implicit session: Session[Interpretation]) = measureExecutionTime {
    val query: Query[Void, String] =
      sql"SELECT token FROM projects_tokens WHERE project_path = #${projectPath.value}".query(varchar)
    SqlQuery(
      Kleisli(_.option(query).map(_.isDefined)),
      name = "associate token - check"
    )
  }

  private def update(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken)(implicit
      session:                  Session[Interpretation]
  ) = measureExecutionTime {
    val query: Command[Void] = sql"""UPDATE projects_tokens
          SET token = #${encryptedToken.value}, project_id = #${projectId.value.toString}
          WHERE project_path = #${projectPath.value} """.command
    SqlQuery(
      Kleisli(_.execute(query).map(failIfMultiUpdate(projectId, projectPath))),
      name = "associate token - update"
    )
  }

  private def insert(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken)(implicit
      session:                  Session[Interpretation]
  ) = measureExecutionTime {
    val query: Command[Void] = sql"""INSERT INTO
          projects_tokens (project_id, project_path, token)
          VALUES (#${projectId.value.toString}, #${projectPath.value}, #${encryptedToken.value})
      """.command
    SqlQuery(
      Kleisli(_.execute(query).map(failIfMultiUpdate(projectId, projectPath))),
      name = "associate token - insert"
    )
  }

  private def failIfMultiUpdate(projectId: Id, projectPath: Path): Completion => Unit = {
    case Update(1) => ()
    case _         => throw new RuntimeException(s"Associating token for project $projectPath ($projectId)")
  }
}

private class IOAssociationPersister(
    transactor:          SessionResource[IO, ProjectsTokensDB],
    queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
)(implicit contextShift: ContextShift[IO])
    extends AssociationPersister[IO](transactor, queriesExecTimes)
