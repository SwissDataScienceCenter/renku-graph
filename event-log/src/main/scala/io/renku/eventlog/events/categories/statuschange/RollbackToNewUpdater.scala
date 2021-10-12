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

import cats.data.Kleisli
import cats.effect.{BracketThrow, Sync}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SqlStatement}
import ch.datascience.graph.model.events.EventId
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, New}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.ExecutionDate
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.RollbackToNew
import skunk.data.Completion
import skunk.implicits._
import skunk.~

import java.time.Instant

private class RollbackToNewUpdater[Interpretation[_]: BracketThrow: Sync](
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[Interpretation, RollbackToNew] {

  override def updateDB(event: RollbackToNew): UpdateResult[Interpretation] = measureExecutionTime {
    SqlStatement[Interpretation](name = "to_new rollback - status update")
      .command[ExecutionDate ~ EventId ~ projects.Id](
        sql"""UPDATE event
              SET status = '#${New.value}', execution_date = $executionDateEncoder
              WHERE event_id = $eventIdEncoder 
                AND project_id = $projectIdEncoder 
                AND status = '#${GeneratingTriples.value}'
               """.command
      )
      .arguments(ExecutionDate(now()) ~ event.eventId.id ~ event.eventId.projectId)
      .build
      .flatMapResult {
        case Completion.Update(1) =>
          DBUpdateResults
            .ForProjects(event.projectPath, Map(GeneratingTriples -> -1, New -> 1))
            .pure[Interpretation]
            .widen[DBUpdateResults]
        case _ =>
          new Exception(s"Could not rollback event ${event.eventId} to status $New")
            .raiseError[Interpretation, DBUpdateResults]
      }
  }

  override def onRollback(event: RollbackToNew) = Kleisli.pure(())
}
