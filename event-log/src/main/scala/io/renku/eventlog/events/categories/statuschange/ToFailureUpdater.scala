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
import ch.datascience.graph.model.events.EventId
import ch.datascience.graph.model.events.EventStatus.{FailureStatus, ProcessingStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ToFailure
import io.renku.eventlog.{EventMessage, ExecutionDate}
import skunk.data.Completion
import skunk.implicits._
import skunk.~

import java.time.Instant

private class ToFailureUpdater[Interpretation[_]: BracketThrow: Sync](
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[Interpretation, ToFailure[ProcessingStatus, FailureStatus]] {

  override def updateDB(event: ToFailure[ProcessingStatus, FailureStatus]): UpdateResult[Interpretation] =
    measureExecutionTime {
      SqlStatement[Interpretation](name =
        Refined.unsafeApply(s"to_${event.newStatus.value.toLowerCase} - status update")
      )
        .command[FailureStatus ~ ExecutionDate ~ EventMessage ~ EventId ~ projects.Id ~ ProcessingStatus](
          sql"""UPDATE event
                SET status = $eventFailureStatusEncoder,
                  execution_date = $executionDateEncoder,
                  message = $eventMessageEncoder
                WHERE event_id = $eventIdEncoder 
                  AND project_id = $projectIdEncoder 
                  AND status = $eventProcessingStatusEncoder
                 """.command
        )
        .arguments(
          event.newStatus ~
            ExecutionDate(now()) ~
            event.message ~
            event.eventId.id ~
            event.eventId.projectId ~
            event.currentStatus
        )
        .build
        .flatMapResult {
          case Completion.Update(1) =>
            DBUpdateResults
              .ForProjects(event.projectPath, Map(event.currentStatus -> -1, event.newStatus -> 1))
              .pure[Interpretation]
              .widen[DBUpdateResults]
          case _ =>
            new Exception(s"Could not update event ${event.eventId} to status ${event.newStatus}")
              .raiseError[Interpretation, DBUpdateResults]
        }
    }
}
