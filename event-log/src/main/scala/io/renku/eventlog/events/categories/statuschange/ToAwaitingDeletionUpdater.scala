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

package io.renku.eventlog.events.categories.statuschange

import cats.effect.{BracketThrow, Sync}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SqlStatement}
import ch.datascience.graph.model.events.EventStatus.AwaitingDeletion
import ch.datascience.graph.model.events.{EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.ExecutionDate
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ToAwaitingDeletion
import skunk.implicits._
import skunk.~

import java.time.Instant

private class ToAwaitingDeletionUpdater[Interpretation[_]: BracketThrow: Sync](
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[Interpretation, ToAwaitingDeletion] {

  override def updateDB(event: ToAwaitingDeletion): UpdateResult[Interpretation] = measureExecutionTime {
    SqlStatement[Interpretation](name = "to_awaiting_deletion - status update")
      .select[ExecutionDate ~ projects.Id ~ EventId, EventStatus](
        sql"""UPDATE event evt
              SET status = '#${AwaitingDeletion.value}', execution_date = $executionDateEncoder
              FROM (
                SELECT event_id, project_id, status 
                FROM event
                WHERE project_id = $projectIdEncoder
                  AND event_id = $eventIdEncoder
                FOR UPDATE
              ) old_evt
              WHERE evt.event_id = old_evt.event_id AND evt.project_id = old_evt.project_id 
              RETURNING old_evt.status
              """.query(eventStatusDecoder)
      )
      .arguments(ExecutionDate(now()) ~ event.eventId.projectId ~ event.eventId.id)
      .build(_.option)
      .flatMapResult {
        case Some(oldEventStatus) =>
          DBUpdateResults
            .ForProjects(event.projectPath, Map(oldEventStatus -> -1, AwaitingDeletion -> 1))
            .pure[Interpretation]
            .widen[DBUpdateResults]
        case _ =>
          new Exception(s"Could not update event ${event.eventId} to status $AwaitingDeletion")
            .raiseError[Interpretation, DBUpdateResults]
      }
  }
}
