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

import TokenDates.{CreatedAt, ExpiryDate}
import cats.data.Kleisli
import cats.effect._
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.graph.model.projects.{Id, Path}
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import io.renku.tokenrepository.repository.TokenRepositoryTypeSerializers
import skunk._
import skunk.data.Completion
import skunk.data.Completion.Delete
import skunk.implicits._

private[repository] trait AssociationPersister[F[_]] {
  def persistAssociation(storingInfo: TokenStoringInfo):         F[Unit]
  def updatePath(project:             TokenStoringInfo.Project): F[Unit]
}

private[repository] object AssociationPersister {
  def apply[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F]): AssociationPersister[F] =
    new AssociationPersisterImpl[F](queriesExecTimes)
}

private class AssociationPersisterImpl[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F])
    extends DbClient[F](Some(queriesExecTimes))
    with AssociationPersister[F]
    with TokenRepositoryTypeSerializers {

  override def persistAssociation(storingInfo: TokenStoringInfo): F[Unit] = SessionResource[F].useWithTransactionK {
    Kleisli { case (_, session) =>
      (deleteAssociation(storingInfo.project) >> insert(storingInfo)).run(session)
    }
  }

  override def updatePath(project: TokenStoringInfo.Project): F[Unit] = SessionResource[F].useK(
    updatePathQuery(project)
  )

  private def deleteAssociation(project: TokenStoringInfo.Project) = measureExecutionTime {
    SqlStatement
      .named("associate token - delete")
      .command[Id ~ Path](sql"""
        DELETE FROM projects_tokens 
        WHERE project_id = $projectIdEncoder OR project_path = $projectPathEncoder
      """.command)
      .arguments(project.id, project.path)
      .build
      .flatMapResult {
        case Delete(_) => ().pure[F]
        case completion =>
          new Exception(
            s"re-associating token for projectId = ${project.id}, projectPath = ${project.path} failed: $completion"
          ).raiseError[F, Unit]
      }
  }

  private def insert(storingInfo: TokenStoringInfo) = measureExecutionTime {
    SqlStatement
      .named("associate token - insert")
      .command[Id ~ Path ~ EncryptedAccessToken ~ CreatedAt ~ ExpiryDate](sql"""
        INSERT INTO projects_tokens (project_id, project_path, token, created_at, expiry_date)
        VALUES ($projectIdEncoder, $projectPathEncoder, $encryptedAccessTokenEncoder, $createdAtEncoder, $expiryDateEncoder)
      """.command)
      .arguments(
        storingInfo.project.id ~ storingInfo.project.path ~ storingInfo.encryptedToken ~ storingInfo.dates.createdAt ~ storingInfo.dates.expiryDate
      )
      .build
      .void
  } recoverWith { case SqlState.UniqueViolation(_) => Kleisli.pure(()) }

  private def updatePathQuery(project: TokenStoringInfo.Project) = measureExecutionTime {
    SqlStatement
      .named("associate token - update path")
      .command[Path ~ Id](sql"""
        UPDATE projects_tokens
        SET project_path = $projectPathEncoder
        WHERE project_id = $projectIdEncoder
    """.command)
      .arguments(project.path ~ project.id)
      .build
      .flatMapResult(failIfMultiUpdate(project.id))
  }

  private def failIfMultiUpdate(projectId: Id): Completion => F[Unit] = {
    case Completion.Update(0 | 1) => ().pure[F]
    case completion =>
      new RuntimeException(s"Updating path for a projectId: $projectId failed with completion code $completion")
        .raiseError[F, Unit]
  }
}
