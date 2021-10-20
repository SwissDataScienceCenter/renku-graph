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

package io.renku.tokenrepository.repository.fetching

import cats.data.OptionT
import cats.effect._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.graph.model.projects.{Id, Path}
import io.renku.metrics.LabeledHistogram
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import io.renku.tokenrepository.repository.{ProjectsTokensDB, TokenRepositoryTypeSerializers}
import skunk.implicits._

private trait PersistedTokensFinder[Interpretation[_]] {
  def findToken(projectId:   Id):   OptionT[Interpretation, EncryptedAccessToken]
  def findToken(projectPath: Path): OptionT[Interpretation, EncryptedAccessToken]
}

private class PersistedTokensFinderImpl[Interpretation[_]: MonadCancelThrow](
    sessionResource:  SessionResource[Interpretation, ProjectsTokensDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name]
) extends DbClient[Interpretation](Some(queriesExecTimes))
    with PersistedTokensFinder[Interpretation]
    with TokenRepositoryTypeSerializers {

  override def findToken(projectId: Id): OptionT[Interpretation, EncryptedAccessToken] = run {
    SqlStatement(name = "find token - id")
      .select[Id, EncryptedAccessToken](
        sql"""select token from projects_tokens where project_id = $projectIdEncoder"""
          .query(encryptedAccessTokenDecoder)
      )
      .arguments(projectId)
      .build(_.option)
  }

  override def findToken(projectPath: Path): OptionT[Interpretation, EncryptedAccessToken] = run {
    SqlStatement(name = "find token - path")
      .select[Path, EncryptedAccessToken](
        sql"select token from projects_tokens where project_path = $projectPathEncoder"
          .query(encryptedAccessTokenDecoder)
      )
      .arguments(projectPath)
      .build(_.option)
  }

  private def run(query: SqlStatement[Interpretation, Option[EncryptedAccessToken]]) = OptionT {
    sessionResource.useK(measureExecutionTime(query))
  }
}
