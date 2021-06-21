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
import ch.datascience.db.{DbClient, SqlStatement}
import ch.datascience.graph.model.events.{EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.ExecutionDate
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.categories.statuschange.DBUpdateResults.ForProject
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.AncestorsToTriplesStore
import skunk.data.Completion
import skunk.implicits._
import skunk.~

import java.time.Instant

private class AncestorsToTriplesStoreUpdater[Interpretation[_]: BracketThrow: Sync](
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[Interpretation, AncestorsToTriplesStore] {

  override def updateDB(
      event: AncestorsToTriplesStore
  ): UpdateResult[Interpretation] = for {
    updatedCount <- updateAncestorsStatus(event)
  } yield ForProject(event.projectPath, Map(EventStatus.TriplesGenerated -> updatedCount))

  private def updateAncestorsStatus(event: AncestorsToTriplesStore) = measureExecutionTime {
    SqlStatement(name = "status_change_event - triples_store")
      .command[ExecutionDate ~ projects.Id ~ projects.Id ~ EventId ~ EventId](
        sql"""UPDATE event evt
              SET status = '#${EventStatus.TriplesStore.value}', 
                  execution_date = $executionDateEncoder, 
                  message = NULL
              WHERE project_id = $projectIdEncoder
                AND status = '#${EventStatus.TriplesGenerated.value}'
                AND event_date <= (
                  SELECT event_date
                  FROM event
                  WHERE project_id = $projectIdEncoder
                    AND event_id = $eventIdEncoder
                )
                AND event_id <> $eventIdEncoder
           """.command
      )
      .arguments(
        ExecutionDate(now()) ~ event.eventId.projectId ~ event.eventId.projectId ~ event.eventId.id ~ event.eventId.id
      )
      .build
      .mapResult {
        case Completion.Update(count) => count
        case _                        => 0
      }

  }

}
