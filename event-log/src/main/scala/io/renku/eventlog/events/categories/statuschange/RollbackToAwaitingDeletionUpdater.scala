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
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.RollbackToAwaitingDeletion
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting}
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk.implicits._
import skunk.~

import java.time.Instant
import skunk.data.Completion

private class RollbackToAwaitingDeletionUpdater[F[_]: MonadCancelThrow](
    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[F, RollbackToAwaitingDeletion] {

  override def updateDB(event: RollbackToAwaitingDeletion): UpdateResult[F] = measureExecutionTime {
    SqlStatement[F](name = "rollback_to_awaiting_deletion - status update")
      .command[EventStatus ~ ExecutionDate ~ projects.Id ~ EventStatus](
        sql"""UPDATE event evt
              SET status = $eventStatusEncoder, execution_date = $executionDateEncoder
              WHERE project_id = $projectIdEncoder
                  AND status = $eventStatusEncoder
              """.command
      )
      .arguments(AwaitingDeletion ~ ExecutionDate(now()) ~ event.project.id ~ Deleting)
      .build
      .flatMapResult {
        case Completion.Update(count) =>
          DBUpdateResults
            .ForProjects(event.project.path, Map(Deleting -> -count, AwaitingDeletion -> count))
            .pure[F]
            .widen[DBUpdateResults]
        case _ =>
          new Exception(
            s"Could not update $Deleting events for project ${event.project.path} to status $AwaitingDeletion"
          )
            .raiseError[F, DBUpdateResults]
      }
  }

  override def onRollback(event: RollbackToAwaitingDeletion) = Kleisli.pure(())
}
