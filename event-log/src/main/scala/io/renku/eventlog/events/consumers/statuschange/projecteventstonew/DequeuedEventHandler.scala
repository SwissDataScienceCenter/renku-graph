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

package io.renku.eventlog.events.consumers.statuschange
package projecteventstonew

import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import cleaning.ProjectCleaner
import eu.timepit.refined.auto._
import io.renku.db.implicits.PreparedQueryOps
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.api.events.StatusChangeEvent.ProjectEventsToNew
import io.renku.eventlog.events.consumers.statuschange.DBUpdater.{RollbackOp, UpdateOp}
import io.renku.eventlog.events.consumers.statuschange.{DBUpdateResults, DBUpdater, categoryName}
import io.renku.eventlog.events.producers.minprojectinfo
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.consumers.Project
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting, GeneratingTriples, New, Skipped}
import io.renku.graph.model.events.{EventDate, EventStatus, ExecutionDate}
import io.renku.graph.model.projects
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant
import scala.util.control.NonFatal

trait DequeuedEventHandler[F[_]] extends DBUpdater[F, ProjectEventsToNew]

object DequeuedEventHandler {

  def apply[F[_]: Async: AccessTokenFinder: Logger: QueriesExecutionTimes: MetricsRegistry]
      : F[DBUpdater[F, ProjectEventsToNew]] =
    ProjectCleaner[F].map(new DequeuedEventHandlerImpl(_))

  private[statuschange] class DequeuedEventHandlerImpl[F[_]: Async: Logger: QueriesExecutionTimes](
      projectCleaner: ProjectCleaner[F],
      now:            () => Instant = () => Instant.now
  ) extends DbClient(Some(QueriesExecutionTimes[F]))
      with DequeuedEventHandler[F] {

    override def updateDB(event: ProjectEventsToNew): UpdateOp[F] =
      for {
        statuses                 <- updateStatuses(event.project)
        _                        <- removeProcessingTimes(event.project)
        _                        <- removePayloads(event.project)
        _                        <- removeDeliveryInfo(event.project)
        _                        <- removeCategorySyncTimes(event.project)
        removedAwaitingDeletions <- removeEvents(event.project, AwaitingDeletion)
        removedDeletingEvents    <- removeEvents(event.project, Deleting)
        maybeLatestEventDate     <- findLatestEventDate(event.project)
        _                        <- updateLatestEventDate(event.project)(maybeLatestEventDate)
        _                        <- cleanUpProjectIfGone(event.project)(maybeLatestEventDate)
      } yield DBUpdateResults
        .ForProjects(event.project.path, eventCountsByStatus(statuses, removedAwaitingDeletions, removedDeletingEvents))
        .widen

    private def eventCountsByStatus(statuses:                 List[EventStatus],
                                    removedAwaitingDeletions: Int,
                                    removedDeletingEvent:     Int
    ) = statuses
      .groupBy(identity)
      .map { case (eventStatus, eventStatuses) => (eventStatus, -1 * eventStatuses.length) }
      .updatedWith(New) { maybeNewEventsCount =>
        maybeNewEventsCount
          .map(_ + statuses.length)
          .orElse(if (statuses.nonEmpty) Some(statuses.length) else None)
      }
      .updated(AwaitingDeletion, -removedAwaitingDeletions)
      .updated(Deleting, -removedDeletingEvent)

    private def updateStatuses(project: Project) = measureExecutionTime {
      SqlStatement(name = "project_to_new - status update")
        .select[ExecutionDate *: projects.GitLabId *: projects.Path *: EmptyTuple, EventStatus](sql"""
          UPDATE event evt
          SET status = '#${New.value}',
              execution_date = $executionDateEncoder,
              message = NULL
          FROM (
            SELECT event_id, e.project_id, status
            FROM event e
            JOIN project p ON e.project_id = p.project_id
              AND p.project_id = $projectIdEncoder
              AND p.project_path = $projectPathEncoder
            WHERE #${`status IN`(EventStatus.all diff Set(Skipped, GeneratingTriples, AwaitingDeletion, Deleting))}
            FOR UPDATE
          ) old_evt
          WHERE evt.event_id = old_evt.event_id AND evt.project_id = old_evt.project_id
          RETURNING old_evt.status
          """.query(eventStatusDecoder))
        .arguments(ExecutionDate(now()) *: project.id *: project.path *: EmptyTuple)
        .build(_.toList)
    }

    private def `status IN`(statuses: Set[EventStatus]) =
      s"status IN (${statuses.map(s => s"'$s'").toList.mkString(",")})"

    private def removeProcessingTimes(project: Project) = measureExecutionTime {
      SqlStatement(name = "project_to_new - processing_times removal")
        .command[projects.GitLabId *: projects.Path *: EmptyTuple](sql"""
          DELETE FROM status_processing_time
          WHERE project_id IN (
            SELECT t.project_id
            FROM status_processing_time t
            JOIN project p ON t.project_id = p.project_id
              AND p.project_id = $projectIdEncoder
              AND p.project_path = $projectPathEncoder
          )""".command)
        .arguments(project.id *: project.path *: EmptyTuple)
        .build
        .void
    }

    private def removePayloads(project: Project) = measureExecutionTime {
      SqlStatement(name = "project_to_new - payloads removal")
        .command[projects.GitLabId *: projects.Path *: EmptyTuple](sql"""
          DELETE FROM event_payload
          WHERE project_id IN (
            SELECT ep.project_id
            FROM event_payload ep
            JOIN project p ON ep.project_id = p.project_id
              AND p.project_id = $projectIdEncoder
              AND p.project_path = $projectPathEncoder
          )""".command)
        .arguments(project.id *: project.path *: EmptyTuple)
        .build
        .void
    }

    private def removeEvents(project: Project, status: EventStatus) = measureExecutionTime {
      SqlStatement
        .named(show"project_to_new - $status removal")
        .command[EventStatus *: projects.GitLabId *: projects.Path *: EmptyTuple](sql"""
          DELETE FROM event
          WHERE status = $eventStatusEncoder AND project_id IN (
            SELECT e.project_id
            FROM event e
            JOIN project p ON e.project_id = p.project_id
              AND p.project_id = $projectIdEncoder
              AND p.project_path = $projectPathEncoder
          )
          """.command)
        .arguments(status *: project.id *: project.path *: EmptyTuple)
        .build
        .mapResult {
          case Completion.Delete(count) => count
          case _                        => 0
        }
    }

    private def removeDeliveryInfo(project: Project) = measureExecutionTime {
      SqlStatement(name = "project_to_new - delivery removal")
        .command[projects.GitLabId *: projects.GitLabId *: projects.Path *: EmptyTuple](sql"""
          DELETE FROM event_delivery
          WHERE project_id = $projectIdEncoder
            AND event_id NOT IN (
              SELECT e.event_id
              FROM event e
              JOIN project p ON e.project_id = p.project_id
                AND p.project_id = $projectIdEncoder
                AND p.project_path = $projectPathEncoder
              WHERE e.status = '#${GeneratingTriples.value}'
            )""".command)
        .arguments(project.id *: project.id *: project.path *: EmptyTuple)
        .build
        .void
    }

    private def removeCategorySyncTimes(project: Project) = measureExecutionTime {
      SqlStatement
        .named("project_to_new - delivery removal")
        .command[projects.GitLabId *: projects.Path *: EmptyTuple](sql"""
          DELETE FROM subscription_category_sync_time
          WHERE category_name = '#${minprojectinfo.categoryName.show}' AND project_id IN (
            SELECT st.project_id
            FROM subscription_category_sync_time st
            JOIN project p ON st.project_id = p.project_id
              AND p.project_id = $projectIdEncoder
              AND p.project_path = $projectPathEncoder
          )
          """.command)
        .arguments(project.id *: project.path *: EmptyTuple)
        .build
        .void
    }

    private def findLatestEventDate(project: Project) = measureExecutionTime {
      SqlStatement(name = "project_to_new - get latest event date")
        .select[projects.GitLabId *: projects.Path *: EmptyTuple, EventDate](sql"""
          SELECT event_date
          FROM event e
          JOIN project p ON e.project_id = p.project_id
            AND p.project_id = $projectIdEncoder
            AND p.project_path = $projectPathEncoder
          ORDER BY event_date DESC
          LIMIT 1""".query(eventDateDecoder))
        .arguments(project.id *: project.path *: EmptyTuple)
        .build(_.option)
    }

    private def updateLatestEventDate(project: Project): Option[EventDate] => Kleisli[F, Session[F], Unit] = {
      case Some(eventDate) =>
        measureExecutionTime {
          SqlStatement(name = "project_to_new - set latest event date")
            .command[EventDate *: projects.GitLabId *: EmptyTuple](sql"""
              UPDATE project
              SET latest_event_date = $eventDateEncoder
              WHERE project_id = $projectIdEncoder
              """.command)
            .arguments(eventDate *: project.id *: EmptyTuple)
            .build
            .void
        }
      case None => Kleisli.pure(())
    }

    private def cleanUpProjectIfGone(project: Project): Option[EventDate] => Kleisli[F, Session[F], Unit] = {
      case Some(_) => Kleisli.pure(())
      case None    => projectCleaner.cleanUp(project) recoverWith logError(project)
    }

    private def logError(project: Project): PartialFunction[Throwable, Kleisli[F, Session[F], Unit]] = {
      case NonFatal(error) =>
        Kleisli.liftF(Logger[F].error(error)(s"$categoryName: project clean up failed: ${project.show}"))
    }

    override def onRollback(event: ProjectEventsToNew): RollbackOp[F] = { case SqlState.DeadlockDetected(_) =>
      Kleisli.liftF[F, Session[F], Unit](
        Logger[F].info(show"$categoryName: deadlock happened while processing $event; retrying")
      ) >> updateDB(event)
    }
  }
}
