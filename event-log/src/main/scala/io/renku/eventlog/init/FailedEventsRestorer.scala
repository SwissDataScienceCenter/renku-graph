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

package io.renku.eventlog.init

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.graph.model.events.EventStatus
import org.typelevel.log4cats.Logger
import skunk._
import skunk.data.Completion
import skunk.implicits._

private trait FailedEventsRestorer[F[_]] extends DbMigrator[F]

private class FailedEventsRestorerImpl[F[_]: MonadCancelThrow: Logger: SessionResource](
    failure:            String,
    currentStatus:      EventStatus,
    destinationStatus:  EventStatus,
    discardingStatuses: List[EventStatus]
) extends FailedEventsRestorer[F] {

  override def run(): F[Unit] = SessionResource[F].useK {
    Kleisli[F, Session[F], Completion](_ execute updateQuery) flatMapF {
      case Completion.Update(count) => Logger[F].info(s"$count events restored for processing from '$failure'")
      case completion => Logger[F].info(s"Events restoration for processing from '$failure' did not work: $completion")
    }
  }

  private lazy val updateQuery: Command[Void] = sql"""
    UPDATE event e
    SET status = '#${destinationStatus.show}', message = NULL
    FROM (
      SELECT evt.event_id, evt.project_id
      FROM (
        SELECT project_id, MAX(event_date) max_event_date
        FROM event
        WHERE status = '#${currentStatus.show}' 
          AND message like '#$failure'
        GROUP BY project_id
      ) candidates
      JOIN event evt ON evt.project_id = candidates.project_id 
        AND evt.status = '#${currentStatus.show}' 
        AND evt.message like '#$failure'
      WHERE NOT EXISTS (
      	SELECT event_id
      	FROM event evte
      	WHERE evte.project_id = candidates.project_id
      	  AND evte.event_date > max_event_date
      	  AND evte.status #${IN(discardingStatuses)}
      )
    ) for_update
    WHERE e.event_id = for_update.event_id 
      AND e.project_id = for_update.project_id 
    """.command

  private def IN(statuses: List[EventStatus]) =
    s"IN (${statuses.map(s => s"'${s.show}'").mkString(",")})"
}
