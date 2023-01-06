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

import ProjectsTokensDB.SessionResource
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import creation.TokenDates.ExpiryDate
import io.renku.db.{DbClient, SqlStatement}
import io.renku.graph.model.projects
import io.renku.tokenrepository.repository.metrics.QueriesExecutionTimes

import java.time.LocalDate.now
import java.time.Period

private trait TokenDueChecker[F[_]] {
  def checkTokenDue(projectId: projects.GitLabId): F[Boolean]
}

private object TokenDueChecker {
  def apply[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]: F[TokenDueChecker[F]] =
    ProjectTokenDuePeriod[F]().map(new TokenDueCheckerImpl[F](_))
}

private class TokenDueCheckerImpl[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes](
    tokenDuePeriod: Period
) extends DbClient[F](Some(QueriesExecutionTimes[F]))
    with TokenDueChecker[F]
    with TokenRepositoryTypeSerializers {

  import skunk.implicits._

  override def checkTokenDue(projectId: projects.GitLabId): F[Boolean] =
    SessionResource[F].useK(measureExecutionTime(query(projectId))).map {
      case None         => false
      case Some(expiry) => (now().plus(tokenDuePeriod) compareTo expiry.value) >= 0
    }

  private def query(projectId: projects.GitLabId) =
    SqlStatement
      .named(name = "find token due")
      .select[projects.GitLabId, ExpiryDate](
        sql"""SELECT expiry_date
              FROM projects_tokens
              WHERE project_id = $projectIdEncoder
              LIMIT 1"""
          .query(expiryDateDecoder)
      )
      .arguments(projectId)
      .build(_.option)
}
