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

package io.renku.eventlog.events.consumers.projectsync

import cats.effect.MonadCancelThrow
import io.renku.db.DbClient
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.graph.model.projects

private trait PayloadFinder[F[_]] {
  def findLatestPayload(projectId: projects.GitLabId): F[Option[ZippedEventPayload]]
}

private object PayloadFinder {
  def apply[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]: PayloadFinder[F] =
    new PayloadFinderImpl[F]
}

private class PayloadFinderImpl[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]
    extends DbClient(Some(QueriesExecutionTimes[F]))
    with PayloadFinder[F] {

  import io.renku.db.SqlStatement
  import io.renku.eventlog.TypeSerializers._
  import skunk._
  import skunk.implicits._

  override def findLatestPayload(projectId: projects.GitLabId): F[Option[ZippedEventPayload]] =
    SessionResource[F].useK(measureExecutionTime(findPayload(projectId)))

  private def findPayload(projectId: projects.GitLabId): SqlStatement[F, Option[ZippedEventPayload]] =
    SqlStatement
      .named("find latest event payload")
      .select(query)
      .arguments(projectId)
      .build(_.option)

  private def query: Query[projects.GitLabId, ZippedEventPayload] =
    sql"""SELECT ep.payload
          FROM event e
          JOIN event_payload ep ON
            e.project_id = ep.project_id
            AND e.event_id = ep.event_id
            AND e.project_id = $projectIdEncoder
          ORDER BY e.event_date DESC
          LIMIT 1"""
      .query(zippedPayloadDecoder)
}
