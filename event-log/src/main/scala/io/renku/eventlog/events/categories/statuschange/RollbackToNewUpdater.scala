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

package io.renku.eventlog.events.categories.statuschange

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.ExecutionDate
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.RollbackToNew
import io.renku.graph.model.events.EventId
import io.renku.graph.model.events.EventStatus.{GeneratingTriples, New}
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk.data.Completion
import skunk.implicits._
import skunk.~

import java.time.Instant

private class RollbackToNewUpdater[F[_]: MonadCancelThrow](
    queriesExecTimes: LabeledHistogram[F],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[F, RollbackToNew] {

  override def updateDB(event: RollbackToNew): UpdateResult[F] = measureExecutionTime {
    SqlStatement[F](name = "to_new rollback - status update")
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
            .pure[F]
            .widen[DBUpdateResults]
        case Completion.Update(0) => DBUpdateResults.ForProjects.empty.pure[F].widen[DBUpdateResults]
        case _ =>
          new Exception(s"Could not rollback event ${event.eventId} to status $New")
            .raiseError[F, DBUpdateResults]
      }
  }

  override def onRollback(event: RollbackToNew) = Kleisli.pure(())
}
