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

import cats.data.Kleisli
import cats.effect._
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.metrics.LabeledHistogram
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import ch.datascience.tokenrepository.repository.{ProjectsTokensDB, TokenRepositoryTypeSerializers}
import eu.timepit.refined.auto._
import skunk._
import skunk.data.Completion
import skunk.data.Completion.{Insert, Update}
import skunk.implicits._

private class AssociationPersister[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    sessionResource:  SessionResource[Interpretation, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name]
) extends DbClient[Interpretation](Some(queriesExecTimes))
    with TokenRepositoryTypeSerializers {

  def persistAssociation(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken): Interpretation[Unit] =
    sessionResource.useK(upsert(projectId, projectPath, encryptedToken))

  private def upsert(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken) =
    checkIfTokenExists(projectPath) flatMap {
      case true  => update(projectId, projectPath, encryptedToken)
      case false => insert(projectId, projectPath, encryptedToken)
    }

  private def checkIfTokenExists(projectPath: Path) = measureExecutionTime {
    SqlQuery(
      Kleisli { session =>
        val query: Query[Path, EncryptedAccessToken] =
          sql"SELECT token FROM projects_tokens WHERE project_path = $projectPathEncoder".query(
            encryptedAccessTokenDecoder
          )
        session.prepare(query).use(_.option(projectPath)).map(_.isDefined)
      },
      name = "associate token - check"
    )
  }

  private def update(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken) = measureExecutionTime {

    SqlQuery(
      Kleisli { session =>
        val query: Command[EncryptedAccessToken ~ Id ~ Path] =
          sql"""UPDATE projects_tokens
                SET token = $encryptedAccessTokenEncoder, project_id = $projectIdEncoder
                WHERE project_path = $projectPathEncoder 
          """.command
        session
          .prepare(query)
          .use(_.execute(encryptedToken ~ projectId ~ projectPath))
          .map(failIfMultiUpdate(projectId, projectPath))

      },
      name = "associate token - update"
    )
  }

  private def insert(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken) = measureExecutionTime {
    SqlQuery(
      Kleisli { session =>
        val query: Command[Id ~ Path ~ EncryptedAccessToken] =
          sql"""INSERT INTO projects_tokens (project_id, project_path, token)
                VALUES ($projectIdEncoder, $projectPathEncoder, $encryptedAccessTokenEncoder)
          """.command
        session
          .prepare(query)
          .use(_.execute(projectId ~ projectPath ~ encryptedToken))
          .map(failIfMultiUpdate(projectId, projectPath))
      },
      name = "associate token - insert"
    )
  }

  private def failIfMultiUpdate(projectId: Id, projectPath: Path): Completion => Unit = {
    case Insert(1) | Update(1) => ()
    case _                     => throw new RuntimeException(s"Associating token for project $projectPath ($projectId)")
  }
}

private class IOAssociationPersister(
    sessionResource:     SessionResource[IO, ProjectsTokensDB],
    queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
)(implicit contextShift: ContextShift[IO])
    extends AssociationPersister[IO](sessionResource, queriesExecTimes)
