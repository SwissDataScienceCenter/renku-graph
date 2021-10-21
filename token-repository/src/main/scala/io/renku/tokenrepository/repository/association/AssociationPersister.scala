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

package io.renku.tokenrepository.repository.association

import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.graph.model.projects.{Id, Path}
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.{ProjectsTokensDB, TokenRepositoryTypeSerializers}
import skunk._
import skunk.data.Completion
import skunk.data.Completion.{Insert, Update}
import skunk.implicits._

private trait AssociationPersister[F[_]] {
  def persistAssociation(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken): F[Unit]
}

private class AssociationPersisterImpl[F[_]: MonadCancelThrow](
    sessionResource:  SessionResource[F, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
) extends DbClient[F](Some(queriesExecTimes))
    with AssociationPersister[F]
    with TokenRepositoryTypeSerializers {

  override def persistAssociation(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken): F[Unit] =
    sessionResource.useK(upsert(projectId, projectPath, encryptedToken))

  private def upsert(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken) =
    checkIfTokenExists(projectPath) flatMap {
      case true  => update(projectId, projectPath, encryptedToken)
      case false => insert(projectId, projectPath, encryptedToken)
    }

  private def checkIfTokenExists(projectPath: Path) = measureExecutionTime {
    SqlStatement(name = "associate token - check")
      .select[Path, EncryptedAccessToken](
        sql"SELECT token FROM projects_tokens WHERE project_path = $projectPathEncoder"
          .query(encryptedAccessTokenDecoder)
      )
      .arguments(projectPath)
      .build(_.option)
      .mapResult(_.isDefined)
  }

  private def update(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken) = measureExecutionTime {
    SqlStatement(name = "associate token - update")
      .command[EncryptedAccessToken ~ Id ~ Path](
        sql"""UPDATE projects_tokens
              SET token = $encryptedAccessTokenEncoder, project_id = $projectIdEncoder
              WHERE project_path = $projectPathEncoder 
          """.command
      )
      .arguments(encryptedToken ~ projectId ~ projectPath)
      .build
      .flatMapResult(failIfMultiUpdate(projectId, projectPath))
  }

  private def insert(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken) = measureExecutionTime {
    SqlStatement(name = "associate token - insert")
      .command[Id ~ Path ~ EncryptedAccessToken](
        sql"""INSERT INTO projects_tokens (project_id, project_path, token)
              VALUES ($projectIdEncoder, $projectPathEncoder, $encryptedAccessTokenEncoder)
          """.command
      )
      .arguments(projectId ~ projectPath ~ encryptedToken)
      .build
      .flatMapResult(failIfMultiUpdate(projectId, projectPath))
  }

  private def failIfMultiUpdate(projectId: Id, projectPath: Path): Completion => F[Unit] = {
    case Insert(1) | Update(1) => ().pure[F]
    case _ =>
      new RuntimeException(s"Associating token for project $projectPath ($projectId)").raiseError[F, Unit]
  }
}
