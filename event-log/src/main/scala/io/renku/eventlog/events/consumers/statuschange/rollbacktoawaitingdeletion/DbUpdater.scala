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

package io.renku.eventlog.events.consumers.statuschange.rollbacktoawaitingdeletion

import cats.effect.MonadCancelThrow
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.consumers.statuschange
import io.renku.eventlog.events.consumers.statuschange.DBUpdateResults
import io.renku.eventlog.events.consumers.statuschange.DBUpdater.{RollbackOp, UpdateOp}
import io.renku.eventlog.api.events.StatusChangeEvent.RollbackToAwaitingDeletion
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting}
import io.renku.graph.model.events.{EventStatus, ExecutionDate}
import io.renku.graph.model.projects
import skunk.data.Completion
import skunk.implicits._
import skunk.~

import java.time.Instant

private[statuschange] class DbUpdater[F[_]: MonadCancelThrow: QueriesExecutionTimes](
    now: () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with statuschange.DBUpdater[F, RollbackToAwaitingDeletion] {

  override def updateDB(event: RollbackToAwaitingDeletion): UpdateOp[F] = measureExecutionTime {
    SqlStatement[F](name = "rollback_to_awaiting_deletion - status update")
      .command[EventStatus ~ ExecutionDate ~ projects.GitLabId ~ EventStatus](
        sql"""UPDATE event evt
              SET status = $eventStatusEncoder, execution_date = $executionDateEncoder
              WHERE project_id = $projectIdEncoder AND status = $eventStatusEncoder
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
          ).raiseError[F, DBUpdateResults]
      }
  }

  override def onRollback(event: RollbackToAwaitingDeletion) = RollbackOp.none
}
