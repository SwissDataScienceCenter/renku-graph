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

package io.renku.tokenrepository.repository
package association

import ProjectsTokensDB.SessionResource
import association.TokenDates.ExpiryDate
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram

import java.time.LocalDate.now
import java.time.Period

private trait TokenDueChecker[F[_]] {
  def checkTokenDue(projectId: projects.Id): F[Boolean]
}

private object TokenDueChecker {
  def apply[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F]): F[TokenDueChecker[F]] =
    ProjectTokenDuePeriod[F]().map(new TokenDueCheckerImpl[F](_, queriesExecTimes))
}

private class TokenDueCheckerImpl[F[_]: MonadCancelThrow: SessionResource](
    tokenDuePeriod:   Period,
    queriesExecTimes: LabeledHistogram[F]
) extends DbClient[F](Some(queriesExecTimes))
    with TokenDueChecker[F]
    with TokenRepositoryTypeSerializers {

  import skunk.implicits._

  override def checkTokenDue(projectId: projects.Id): F[Boolean] =
    SessionResource[F].useK(measureExecutionTime(query(projectId))).map {
      case None         => false
      case Some(expiry) => (now().plus(tokenDuePeriod) compareTo expiry.value) >= 0
    }

  private def query(projectId: projects.Id) =
    SqlStatement
      .named(name = "find token due")
      .select[projects.Id, ExpiryDate](
        sql"""SELECT expiry_date
              FROM projects_tokens
              WHERE project_id = $projectIdEncoder
              LIMIT 1"""
          .query(expiryDateDecoder)
      )
      .arguments(projectId)
      .build(_.option)
}
