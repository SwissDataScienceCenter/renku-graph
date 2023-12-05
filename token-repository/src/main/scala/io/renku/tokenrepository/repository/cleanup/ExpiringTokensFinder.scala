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
package cleanup

import ProjectsTokensDB.SessionResource
import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import io.renku.db.implicits._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.events.consumers.Project
import io.renku.graph.model.projects
import io.renku.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import metrics.QueriesExecutionTimes
import skunk.codec.all.date
import skunk.implicits._
import skunk.~

import java.time.{LocalDate, Period}

private trait ExpiringTokensFinder[F[_]] {
  def findExpiringTokens: Stream[F, ExpiringToken]
}

private object ExpiringTokensFinder {

  private val chunkSize:              Int    = 10
  private val periodBeforeExpiration: Period = Period.ofDays(10)

  def apply[F[_]: Async: SessionResource: QueriesExecutionTimes]: F[ExpiringTokensFinder[F]] =
    AccessTokenCrypto[F]().map(new ExpiringTokensFinderImpl[F](_, periodBeforeExpiration, chunkSize))
}

private class ExpiringTokensFinderImpl[F[_]: Async: SessionResource: QueriesExecutionTimes](
    accessTokenCrypto:      AccessTokenCrypto[F],
    periodBeforeExpiration: Period,
    chunkSize:              Int
) extends DbClient[F](Some(QueriesExecutionTimes[F]))
    with ExpiringTokensFinder[F]
    with TokenRepositoryTypeSerializers {

  override def findExpiringTokens: Stream[F, ExpiringToken] =
    fetchAllChunks
      .flatMap(in => Stream.emits(in))

  private def fetchAllChunks: Stream[F, List[ExpiringToken]] =
    (Stream.eval(SessionResource[F].useK(measureExecutionTime(query))) ++ fetchAllChunks)
      .takeWhile(_.nonEmpty)

  private lazy val query =
    SqlStatement
      .named("find expiring tokens")
      .select[LocalDate, (Project, EncryptedAccessToken)](
        sql"""SELECT project_id, project_slug, token
              FROM projects_tokens
              WHERE expiry_date <= $date
              LIMIT #${chunkSize.toString}"""
          .query(projectIdDecoder ~ projectSlugDecoder ~ encryptedAccessTokenDecoder)
          .map { case (id: projects.GitLabId) ~ (slug: projects.Slug) ~ (token: EncryptedAccessToken) =>
            Project(id, slug) -> token
          }
      )
      .arguments(LocalDate.now() plus periodBeforeExpiration)
      .build(_.toList)
      .flatMapResult {
        _.map { case (project, encToken) =>
          accessTokenCrypto
            .decrypt(encToken)
            .map(t => ExpiringToken.Decryptable(project, t).widen)
            .handleError(_ => ExpiringToken.NonDecryptable(project, encToken).widen)
        }.sequence
      }
}
