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
package refresh

import AccessTokenCrypto.EncryptedAccessToken
import ProjectsTokensDB.SessionResource
import association.Project
import cats.effect.MonadCancelThrow
import io.renku.db.{DbClient, SqlStatement}
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram

import java.time.LocalDate

private trait EventsFinder[F[_]] {
  def findEvent(): F[Option[TokenCloseExpiration]]
}

private object EventsFinder {
  def apply[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F]): EventsFinder[F] =
    new EventsFinderImpl[F](queriesExecTimes)
}

private class EventsFinderImpl[F[_]: MonadCancelThrow: SessionResource](
    queriesExecTimes: LabeledHistogram[F]
) extends DbClient[F](Some(queriesExecTimes))
    with EventsFinder[F]
    with TokenRepositoryTypeSerializers {

  import skunk._
  import skunk.codec.all.date
  import skunk.implicits._

  override def findEvent(): F[Option[TokenCloseExpiration]] =
    SessionResource[F].useK(measureExecutionTime(query))

  private def query =
    SqlStatement
      .named(name = "find token close expiration")
      .select[LocalDate, TokenCloseExpiration](
        sql"""SELECT project_id, project_path, token
              FROM projects_tokens
              WHERE expiry_date <= $date
              LIMIT 1"""
          .query(projectIdDecoder ~ projectPathDecoder ~ encryptedAccessTokenDecoder)
          .map { case (id: projects.Id) ~ (path: projects.Path) ~ (token: EncryptedAccessToken) =>
            TokenCloseExpiration(Project(id, path), token)
          }
      )
      .arguments(CloseExpirationDate().value)
      .build(_.option)
}
