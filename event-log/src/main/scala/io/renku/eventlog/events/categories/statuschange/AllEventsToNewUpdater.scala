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
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.ExecutionDate
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.AllEventsToNew
import skunk.Session
import skunk.implicits._

import java.time.Instant

private class AllEventsToNewUpdater[Interpretation[_]: BracketThrow: Sync](
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[Interpretation, AllEventsToNew] {

  override def updateDB(event: AllEventsToNew): UpdateResult[Interpretation] = for {
    _ <- updateStatuses()
    _ <- cleanUp()
  } yield DBUpdateResults.ForAllProjects

  private def cleanUp(): Kleisli[Interpretation, Session[Interpretation], Unit] = Kleisli { session =>
    List(
      removeAllProcessingTimes().run(session),
      removeAllPayloads().run(session)
//      removeAwaitingDeletionEvents().run(session)
    ).sequence.void
  }

  private def updateStatuses() = measureExecutionTime {
    SqlStatement(name = "status_change_event - triples_generated")
      .command[ExecutionDate](
        sql"""UPDATE event evt
              SET status = '#${EventStatus.New.value}', 
                  execution_date = $executionDateEncoder, 
                  message = NULL
              WHERE #${`status IN`(EventStatus.all.filterNot(_ == EventStatus.Skipped))} 
           """.command
      )
      .arguments(ExecutionDate(now()))
      .build

  }

  private def removeAllPayloads() =
    measureExecutionTime {
      SqlStatement(name = "status_change_event - payload_removal")
        .command[skunk.Void](
          sql"""TRUNCATE TABLE event_payload""".command
        )
        .arguments(skunk.Void)
        .build
        .void
    }

  private def removeAllProcessingTimes() =
    measureExecutionTime {
      SqlStatement(name = "status_change_event - processing_time_removal")
        .command[skunk.Void](
          sql"""TRUNCATE TABLE status_processing_time""".command
        )
        .arguments(skunk.Void)
        .build
        .void
    }

//  private def removeAwaitingDeletionEvents() =
//        measureExecutionTime {
//          SqlStatement(name = "status_change_event - awaiting_deletion_event_removal")
//            .command[projects.Id](
//              sql"""DELETE FROM event
//              WHERE event_id IN (#${eventIdsToRemove.map(id => s"'$id'").mkString(",")})
//                AND project_id = $projectIdEncoder""".command
//            )
//            .arguments(projectId)
//            .build
//            .void
//    }

  private def `status IN`(statuses: Set[EventStatus]) =
    s"status IN (${statuses.map(s => s"'$s'").toList.mkString(",")})"
}
