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
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.implicits.PreparedQueryOps
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.categories.statuschange.StatusChangeEvent.ProjectEventsToNew
import io.renku.eventlog.events.categories.statuschange.projectCleaner.ProjectCleaner
import io.renku.eventlog.{EventDate, ExecutionDate}
import io.renku.events.consumers.Project
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram
import org.typelevel.log4cats.Logger
import skunk.implicits._
import skunk.{Session, ~}

import java.time.Instant
import scala.util.control.NonFatal

private class ProjectEventsToNewUpdater[F[_]: Async: Logger](
    projectCleaner:   ProjectCleaner[F],
    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with DBUpdater[F, ProjectEventsToNew] {

  override def updateDB(event: ProjectEventsToNew): UpdateResult[F] = for {
    statuses             <- updateStatuses(event.project)
    _                    <- removeProjectProcessingTimes(event.project)
    _                    <- removeProjectPayloads(event.project)
    _                    <- removeProjectDeliveryInfo(event.project)
    _                    <- removeProjectDeletingEvents(event.project)
    maybeLatestEventDate <- getLatestEventDate(event.project)
    _ <- maybeLatestEventDate match {
           case Some(latestEventDate) => updateLatestEventDate(event.project, latestEventDate)
           case None                  => projectCleaner.cleanUp(event.project) recoverWith logError(event.project)
         }
    counts = statuses
               .groupBy(identity)
               .map { case (eventStatus, eventStatuses) => (eventStatus, -1 * eventStatuses.length) }
               .updatedWith(EventStatus.New) { maybeNewEventsCount =>
                 maybeNewEventsCount
                   .map(_ + statuses.length)
                   .orElse(if (statuses.nonEmpty) Some(statuses.length) else None)
               }
  } yield DBUpdateResults.ForProjects(event.project.path, counts)

  private def updateStatuses(project: Project) = measureExecutionTime {
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
          EventStatus.all.diff(Set(EventStatus.Skipped, EventStatus.AwaitingDeletion, EventStatus.Deleting))
        )}
                FOR UPDATE
              ) old_evt
              WHERE evt.event_id = old_evt.event_id AND evt.project_id = old_evt.project_id 
              RETURNING old_evt.status
           """.query(eventStatusDecoder)
      )
      .arguments(ExecutionDate(now()) ~ project.id)
      .build(_.toList)
  }

  private def `status IN`(statuses: Set[EventStatus]) =
    s"status IN (${statuses.map(s => s"'$s'").toList.mkString(",")})"

  private def removeProjectProcessingTimes(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - processing_times removal")
      .command[projects.Id](
        sql"""DELETE FROM status_processing_time WHERE project_id = $projectIdEncoder""".command
      )
      .arguments(project.id)
      .build
      .void
  }

  private def removeProjectPayloads(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - payloads removal")
      .command[projects.Id](
        sql"""DELETE FROM event_payload WHERE project_id = $projectIdEncoder""".command
      )
      .arguments(project.id)
      .build
      .void
  }

  private def removeProjectDeletingEvents(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - awaiting_deletions removal")
      .command[EventStatus ~ projects.Id](
        sql"""DELETE FROM event
              WHERE status = $eventStatusEncoder AND project_id = $projectIdEncoder
        """.command
      )
      .arguments(EventStatus.Deleting ~ project.id)
      .build
      .void
  }

  private def removeProjectDeliveryInfo(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - delivery removal")
      .command[projects.Id](
        sql"""DELETE FROM event_delivery WHERE project_id = $projectIdEncoder""".command
      )
      .arguments(project.id)
      .build
      .void
  }

  private def getLatestEventDate(project: Project) = measureExecutionTime {
    SqlStatement(name = "project_to_new - get latest event date")
      .select[projects.Id, EventDate](
        sql"""SELECT event_date FROM event 
              WHERE project_id = $projectIdEncoder 
              ORDER BY event_date DESC
              LIMIT 1""".query(eventDateDecoder)
      )
      .arguments(project.id)
      .build(_.option)
  }

  private def updateLatestEventDate(project: Project, eventDate: EventDate) = measureExecutionTime {
    SqlStatement(name = "project_to_new - set latest event date")
      .command[EventDate ~ projects.Id](
        sql"""UPDATE project
              SET latest_event_date = $eventDateEncoder
              WHERE project_id = $projectIdEncoder
              """.command
      )
      .arguments(eventDate ~ project.id)
      .build
      .void
  }
  private def logError(project: Project): PartialFunction[Throwable, Kleisli[F, Session[F], Unit]] = {
    case NonFatal(error) =>
      Kleisli.liftF(Logger[F].error(error)(s"Clean up project failed: ${project.show}"))
  }
  override def onRollback(event: ProjectEventsToNew): Kleisli[F, Session[F], Unit] = Kleisli.pure(())
}
