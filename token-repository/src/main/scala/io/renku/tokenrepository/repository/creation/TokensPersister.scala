/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository
package creation

import AccessTokenCrypto.EncryptedAccessToken
import ProjectsTokensDB.SessionResource
import TokenDates.{CreatedAt, ExpiryDate}
import cats.data.Kleisli
import cats.effect._
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.graph.model.projects.{GitLabId, Slug}
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes
import org.typelevel.twiddles.syntax._
import skunk._
import skunk.data.Completion
import skunk.data.Completion.Delete
import skunk.implicits._

private[repository] trait TokensPersister[F[_]] {
  def persistToken(storingInfo: TokenStoringInfo): F[Unit]
  def updateSlug(project:       Project):          F[Unit]
}

private[repository] object TokensPersister {
  def apply[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]: TokensPersister[F] =
    new TokensPersisterImpl[F]
}

private class TokensPersisterImpl[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]
    extends DbClient[F](Some(QueriesExecutionTimes[F]))
    with TokensPersister[F]
    with TokenRepositoryTypeSerializers {

  override def persistToken(storingInfo: TokenStoringInfo): F[Unit] = SessionResource[F].useWithTransactionK {
    Kleisli { case (_, session) =>
      (deleteAssociation(storingInfo.project) >> insert(storingInfo)).run(session)
    }
  }

  override def updateSlug(project: Project): F[Unit] = SessionResource[F].useK(
    updateSlugQuery(project)
  )

  private def deleteAssociation(project: Project) = measureExecutionTime {
    SqlStatement
      .named("associate token - delete")
      .command[GitLabId *: Slug *: EmptyTuple](sql"""
        DELETE FROM projects_tokens 
        WHERE project_id = $projectIdEncoder OR project_path = $projectSlugEncoder
      """.command)
      .arguments(project.id, project.slug)
      .build
      .flatMapResult {
        case Delete(_) => ().pure[F]
        case completion =>
          new Exception(
            s"re-associating token for projectId = ${project.id}, projectSlug = ${project.slug} failed: $completion"
          ).raiseError[F, Unit]
      }
  }

  private def insert(storingInfo: TokenStoringInfo) = measureExecutionTime {
    SqlStatement
      .named("associate token - insert")
      .command[GitLabId *: Slug *: EncryptedAccessToken *: CreatedAt *: ExpiryDate *: EmptyTuple](sql"""
        INSERT INTO projects_tokens (project_id, project_path, token, created_at, expiry_date)
        VALUES ($projectIdEncoder, $projectSlugEncoder, $encryptedAccessTokenEncoder, $createdAtEncoder, $expiryDateEncoder)
      """.command)
      .arguments(
        (storingInfo.project.id,
         storingInfo.project.slug,
         storingInfo.encryptedToken,
         storingInfo.dates.createdAt,
         storingInfo.dates.expiryDate
        )
      )
      .build
      .void
  } recoverWith { case SqlState.UniqueViolation(_) => Kleisli.pure(()) }

  private def updateSlugQuery(project: Project) = measureExecutionTime {
    SqlStatement
      .named("associate token - update slug")
      .command[Slug *: GitLabId *: EmptyTuple](sql"""
        UPDATE projects_tokens
        SET project_path = $projectSlugEncoder
        WHERE project_id = $projectIdEncoder
    """.command)
      .arguments(project.slug -> project.id)
      .build
      .flatMapResult(failIfMultiUpdate(project.id))
  }

  private def failIfMultiUpdate(projectId: GitLabId): Completion => F[Unit] = {
    case Completion.Update(0 | 1) => ().pure[F]
    case completion =>
      new RuntimeException(s"Updating slug for a projectId: $projectId failed with completion code $completion")
        .raiseError[F, Unit]
  }
}
