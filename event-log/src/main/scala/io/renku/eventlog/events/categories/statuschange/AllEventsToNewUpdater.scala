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
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.ExecutionDate
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.AllEventsToNew
import io.renku.graph.model.events.EventStatus
import io.renku.metrics.LabeledHistogram
import skunk.implicits._

import java.time.Instant

private class AllEventsToNewUpdater[Interpretation[_]: BracketThrow: Sync](
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[Interpretation, AllEventsToNew] {

  override def updateDB(event: AllEventsToNew): UpdateResult[Interpretation] = for {
    _ <- updateStatuses()
    _ <- removeAllProcessingTimes()
    _ <- removeAllPayloads()
    _ <- removeAllDeliveryInfos()
    _ <- removeAwaitingDeletionEvents()
  } yield DBUpdateResults.ForAllProjects

  override def onRollback(event: AllEventsToNew) = Kleisli.pure(())

  private def updateStatuses() = measureExecutionTime {
    SqlStatement(name = "all_to_new - status update")
      .command[ExecutionDate](
        sql"""UPDATE event evt
              SET status = '#${EventStatus.New.value}', 
                  execution_date = $executionDateEncoder, 
                  message = NULL
              WHERE #${`status IN`(EventStatus.all.diff(Set(EventStatus.Skipped, EventStatus.AwaitingDeletion)))} 
           """.command
      )
      .arguments(ExecutionDate(now()))
      .build
  }

  private def removeAllProcessingTimes() = measureExecutionTime {
    SqlStatement(name = "all_to_new - processing_times removal")
      .command[skunk.Void](
        sql"""TRUNCATE TABLE status_processing_time""".command
      )
      .arguments(skunk.Void)
      .build
      .void
  }

  private def removeAllPayloads() = measureExecutionTime {
    SqlStatement(name = "all_to_new - payloads removal")
      .command[skunk.Void](
        sql"""TRUNCATE TABLE event_payload""".command
      )
      .arguments(skunk.Void)
      .build
      .void
  }

  private def removeAwaitingDeletionEvents() = measureExecutionTime {
    SqlStatement(name = "all_to_new - awaiting_deletions removal")
      .command[skunk.Void](
        sql"""DELETE FROM event
              WHERE event_id = '#${EventStatus.AwaitingDeletion.value}'
        """.command
      )
      .arguments(skunk.Void)
      .build
      .void
  }

  private def removeAllDeliveryInfos() = measureExecutionTime {
    SqlStatement(name = "all_to_new - delivery removal")
      .command[skunk.Void](
        sql"""TRUNCATE TABLE event_delivery""".command
      )
      .arguments(skunk.Void)
      .build
      .void
  }

  private def `status IN`(statuses: Set[EventStatus]) =
    s"status IN (${statuses.map(s => s"'$s'").toList.mkString(",")})"
}
