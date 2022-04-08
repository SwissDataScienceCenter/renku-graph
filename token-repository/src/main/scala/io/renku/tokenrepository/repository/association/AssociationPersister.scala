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

import cats.data.Kleisli
import cats.effect._
import cats.syntax.all._
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.graph.model.projects.{Id, Path}
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.{ProjectsTokensDB, TokenRepositoryTypeSerializers}
import skunk._
import skunk.data.Completion.Delete
import skunk.implicits._

private trait AssociationPersister[F[_]] {
  def persistAssociation(projectId: Id, projectPath: Path, encryptedToken: EncryptedAccessToken): F[Unit]
}

private class AssociationPersisterImpl[F[_]: MonadCancelThrow](
    sessionResource:  SessionResource[F, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[F]
) extends DbClient[F](Some(queriesExecTimes))
    with AssociationPersister[F]
    with TokenRepositoryTypeSerializers {

  override def persistAssociation(id: Id, path: Path, token: EncryptedAccessToken): F[Unit] =
    sessionResource.useWithTransactionK {
      Kleisli { case (_, session) =>
        (deleteAssociation(id, path) >> insert(id, path, token)).run(session)
      }
    }

  private def deleteAssociation(id: Id, path: Path) = measureExecutionTime {
    SqlStatement
      .named("associate token - delete")
      .command[Id ~ Path](sql"""
        DELETE FROM projects_tokens 
        WHERE project_id = $projectIdEncoder OR project_path = $projectPathEncoder
      """.command)
      .arguments(id, path)
      .build
      .flatMapResult {
        case Delete(_) => ().pure[F]
        case completion =>
          new Exception(s"Re-associating token for projectId = $id, projectPath = $path failed: $completion")
            .raiseError[F, Unit]
      }
  }

  private def insert(id: Id, path: Path, token: EncryptedAccessToken) = measureExecutionTime {
    SqlStatement
      .named("associate token - insert")
      .command[Id ~ Path ~ EncryptedAccessToken](sql"""
        INSERT INTO projects_tokens (project_id, project_path, token)
        VALUES ($projectIdEncoder, $projectPathEncoder, $encryptedAccessTokenEncoder)
      """.command)
      .arguments(id ~ path ~ token)
      .build
      .void
  } recoverWith { case SqlState.UniqueViolation(_) => Kleisli.pure(()) }
}
