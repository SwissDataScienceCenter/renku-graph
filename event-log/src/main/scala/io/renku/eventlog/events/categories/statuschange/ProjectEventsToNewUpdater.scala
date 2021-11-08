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
import cats.effect.Async
import eu.timepit.refined.auto._
import io.renku.db.implicits.PreparedQueryOps
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.ExecutionDate
import io.renku.eventlog.TypeSerializers.{eventStatusDecoder, executionDateEncoder, projectIdEncoder}
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ProjectEventsToNew
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import skunk.implicits._
import skunk.~

import java.time.Instant

private class ProjectEventsToNewUpdater[F[_]: Async](
    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[F, ProjectEventsToNew] {

  override def updateDB(event: ProjectEventsToNew): UpdateResult[F] = for {
    statuses <- updateStatuses(event)
//    _ <- removeProjectProcessingTimes(event)
//    _ <- removeProjectPayloads(event)
//    _ <- removeProjectDeliveryInfos(event)
//    _ <- removeAwaitingDeletionEvents(event)
    counts = statuses
               .groupBy(identity)
               .map { case (eventStatus, eventStatuses) => (eventStatus, -1 * eventStatuses.length) }
               .updatedWith(EventStatus.New) { maybeNewEventsCount =>
                 maybeNewEventsCount.map(_ + statuses.length).orElse(Some(statuses.length))
               }
  } yield DBUpdateResults.ForProjects(event.project.path, counts)

  override def onRollback(event: ProjectEventsToNew) = Kleisli.pure(())

  private def updateStatuses(event: ProjectEventsToNew) = measureExecutionTime {
    SqlStatement(name = "project_to_new - status update")
      .select[ExecutionDate ~ projects.Id, EventStatus](
        sql"""UPDATE event evt
              SET status = '#${EventStatus.New.value}',
                  execution_date = $executionDateEncoder,
                  message = NULL
              FROM (
                SELECT event_id, project_id, status 
                FROM event
                WHERE project_id = $projectIdEncoder AND #${`status IN`(
          EventStatus.all.diff(Set(EventStatus.Skipped, EventStatus.AwaitingDeletion))
        )}
                FOR UPDATE
              ) old_evt
              WHERE evt.event_id = old_evt.event_id AND evt.project_id = old_evt.project_id 
              RETURNING old_evt.status
           """.query(eventStatusDecoder)
      )
      .arguments(ExecutionDate(now()) ~ event.project.id)
      .build(_.toList)

  }
//
//  private def removeProjectProcessingTimes(event: ProjectEventsToNew) = measureExecutionTime {
//    SqlStatement(name = "project_to_new - processing_times removal")
//      .command[skunk.Void](
//        sql"""TRUNCATE TABLE status_processing_time""".command
//      )
//      .arguments(skunk.Void)
//      .build
//      .void
//  }
//
//  private def removeProjectPayloads(event: ProjectEventsToNew) = measureExecutionTime {
//    SqlStatement(name = "project_to_new - payloads removal")
//      .command[skunk.Void](
//        sql"""TRUNCATE TABLE event_payload""".command
//      )
//      .arguments(skunk.Void)
//      .build
//      .void
//  }
//
//  private def removeAwaitingDeletionEvents(event: ProjectEventsToNew) = measureExecutionTime {
//    SqlStatement(name = "project_to_new - awaiting_deletions removal")
//      .command[skunk.Void](
//        sql"""DELETE FROM event
//              WHERE event_id = '#${EventStatus.AwaitingDeletion.value}'
//        """.command
//      )
//      .arguments(skunk.Void)
//      .build
//      .void
//  }
//
//  private def removeProjectDeliveryInfos(event: ProjectEventsToNew) = measureExecutionTime {
//    SqlStatement(name = "project_to_new - delivery removal")
//      .command[skunk.Void](
//        sql"""TRUNCATE TABLE event_delivery""".command
//      )
//      .arguments(skunk.Void)
//      .build
//      .void
//  }
//
  private def `status IN`(statuses: Set[EventStatus]) =
    s"status IN (${statuses.map(s => s"'$s'").toList.mkString(",")})"
}
