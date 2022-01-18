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

package io.renku.eventlog.subscriptions.cleanup

import cats.MonadThrow
import cats.syntax.all._
import io.renku.eventlog.subscriptions.{EventFinder, SubscriptionTypeSerializers}
import io.renku.metrics.LabeledHistogram
import io.renku.db.SqlStatement
import io.renku.db.DbClient
import io.renku.db.SessionResource
import io.renku.metrics.LabeledGauge
import java.time.Instant
import io.renku.eventlog.EventLogDB
import io.renku.graph.model.projects
import cats.effect.Async
import cats.Parallel
import io.renku.eventlog.TypeSerializers
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting}
import io.renku.eventlog.ExecutionDate
import eu.timepit.refined.auto._
import eu.timepit.refined.api.Refined

import skunk._
import skunk.implicits._
import io.renku.graph.model.events
import io.renku.events.consumers.Project
import cats.data.Kleisli
import skunk.data.Completion

private class CleanUpEventFinderImpl[F[_]: Async: Parallel](
    sessionResource:       SessionResource[F, EventLogDB],
    awaitingDeletionGauge: LabeledGauge[F, projects.Path],
    deletingGauge:         LabeledGauge[F, projects.Path],
    queriesExecTimes:      LabeledHistogram[F, SqlStatement.Name],
    now:                   () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[F, CleanUpEvent]
    with SubscriptionTypeSerializers
    with TypeSerializers {

  override def popEvent(): F[Option[CleanUpEvent]] = sessionResource.useK {
    for {
      maybeCleanUpEventAndCount <- findEventAndUpdateForProcessing
      _                         <- maybeUpdateMetrics(maybeCleanUpEventAndCount)
    } yield maybeCleanUpEventAndCount.map(_._1)
  }

  private def findEventAndUpdateForProcessing = for {
    maybeCleanUpEvent <- findEvent
    updatedRows       <- updateForProcessing(maybeCleanUpEvent)
  } yield maybeCleanUpEvent.map(cleanUpEvent => (cleanUpEvent, updatedRows))

  private def findEvent = measureExecutionTime {

    val executionDate = ExecutionDate(now())
    SqlStatement(
      name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find oldest")
    ).select[events.EventStatus ~ ExecutionDate, CleanUpEvent](
      sql"""
       SELECT evt.project_id, prj.project_path
       FROM event evt
       JOIN  project prj ON prj.project_id = evt.project_id 
       WHERE evt.status = $eventStatusEncoder
         AND evt.execution_date < $executionDateEncoder
       ORDER BY evt.event_date ASC
       LIMIT 1
       """
        .query(cleanUpEventGet)
    ).arguments(AwaitingDeletion ~ executionDate)
      .build(_.option)
  }

  private def updateForProcessing(maybeCleanUpEvent: Option[CleanUpEvent]) =
    maybeCleanUpEvent map { case CleanUpEvent(Project(projectId, _)) =>
      measureExecutionTime {
        SqlStatement(
          name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - update status")
        ).command[events.EventStatus ~ ExecutionDate ~ events.EventStatus ~ projects.Id](
          sql"""
           UPDATE event
           SET status = $eventStatusEncoder, execution_date = $executionDateEncoder
           WHERE status = $eventStatusEncoder
             AND project_id = $projectIdEncoder
       """.command
        ).arguments(Deleting ~ ExecutionDate(now()) ~ AwaitingDeletion ~ projectId)
          .build
          .flatMapResult {
            case Completion.Update(updatedRows) => updatedRows.pure[F]
            case completion =>
              new Exception(
                s"Could not update events to status $Deleting for project: $projectId. Value returned was : $completion"
              ).raiseError[F, Int]
          }
      }
    } getOrElse Kleisli.pure[F, Session[F], Int](0)

  private def maybeUpdateMetrics(maybeCleanUpEventAndCount: Option[(CleanUpEvent, Int)]) =
    maybeCleanUpEventAndCount map { case (CleanUpEvent(Project(_, projectPath)), updatedEventsCount) =>
      Kleisli.liftF {
        for {
          _ <- awaitingDeletionGauge.update((projectPath, updatedEventsCount.toDouble * -1))
          _ <- deletingGauge.update((projectPath, updatedEventsCount))
        } yield ()
      }
    } getOrElse Kleisli.pure[F, Session[F], Unit](())

  private val cleanUpEventGet: Decoder[CleanUpEvent] =
    projectDecoder.gmap[CleanUpEvent]
}

private object CleanUpEventFinder {
  def apply[F[_]: Async: Parallel](
      sessionResource:      SessionResource[F, EventLogDB],
      awatingDeletionGauge: LabeledGauge[F, projects.Path],
      deletingGauge:        LabeledGauge[F, projects.Path],
      queriesExecTimes:     LabeledHistogram[F, SqlStatement.Name]
  ): F[EventFinder[F, CleanUpEvent]] = MonadThrow[F].catchNonFatal {
    new CleanUpEventFinderImpl(sessionResource, awatingDeletionGauge, deletingGauge, queriesExecTimes)
  }
}
