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

package io.renku.tokenrepository.repository.fetching

import cats.data.OptionT
import cats.effect._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.graph.model.projects.{Id, Path}
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.ProjectsTokensDB.SessionResource
import io.renku.tokenrepository.repository.TokenRepositoryTypeSerializers
import skunk.implicits._

private[repository] trait PersistedTokensFinder[F[_]] {
  def findStoredToken(projectId:   Id):   OptionT[F, EncryptedAccessToken]
  def findStoredToken(projectPath: Path): OptionT[F, EncryptedAccessToken]
}

private[repository] object PersistedTokensFinder {
  def apply[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F]): PersistedTokensFinder[F] =
    new PersistedTokensFinderImpl[F](queriesExecTimes)
}

private class PersistedTokensFinderImpl[F[_]: MonadCancelThrow: SessionResource](
    queriesExecTimes: LabeledHistogram[F]
) extends DbClient[F](Some(queriesExecTimes))
    with PersistedTokensFinder[F]
    with TokenRepositoryTypeSerializers {

  override def findStoredToken(projectId: Id): OptionT[F, EncryptedAccessToken] = run {
    SqlStatement(name = "find token - id")
      .select[Id, EncryptedAccessToken](
        sql"""select token from projects_tokens where project_id = $projectIdEncoder"""
          .query(encryptedAccessTokenDecoder)
      )
      .arguments(projectId)
      .build(_.option)
  }

  override def findStoredToken(projectPath: Path): OptionT[F, EncryptedAccessToken] = run {
    SqlStatement(name = "find token - path")
      .select[Path, EncryptedAccessToken](
        sql"select token from projects_tokens where project_path = $projectPathEncoder"
          .query(encryptedAccessTokenDecoder)
      )
      .arguments(projectPath)
      .build(_.option)
  }

  private def run(query: SqlStatement[F, Option[EncryptedAccessToken]]) = OptionT {
    SessionResource[F].useK(measureExecutionTime(query))
  }
}
