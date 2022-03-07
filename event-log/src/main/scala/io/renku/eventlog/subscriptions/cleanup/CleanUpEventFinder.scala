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

import cats.{MonadThrow, Parallel}
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.{ExecutionDate, TypeSerializers}
import io.renku.eventlog.subscriptions.{EventFinder, SubscriptionTypeSerializers}
import io.renku.events.consumers.Project
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting}
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram}
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private class CleanUpEventFinderImpl[F[_]: Async: Parallel: SessionResource](
    awaitingDeletionGauge: LabeledGauge[F, projects.Path],
    deletingGauge:         LabeledGauge[F, projects.Path],
    queriesExecTimes:      LabeledHistogram[F, SqlStatement.Name],
    now:                   () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[F, CleanUpEvent]
    with SubscriptionTypeSerializers
    with TypeSerializers {

  override def popEvent(): F[Option[CleanUpEvent]] = SessionResource[F].useK {
    for {
      maybeCleanUpEvent <- findEventAndUpdateForProcessing
      _                 <- maybeUpdateMetrics(maybeCleanUpEvent)
    } yield maybeCleanUpEvent.map(_._1)
  }

  private def findEventAndUpdateForProcessing = for {
    maybeProject      <- findProject
    maybeCleanUpEvent <- updateForProcessing(maybeProject)
  } yield maybeCleanUpEvent

  private def findProject = measureExecutionTime {

    val executionDate = ExecutionDate(now())
    SqlStatement(
      name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find oldest")
    ).select[EventStatus ~ ExecutionDate, Project](
      sql"""
       SELECT evt.project_id, prj.project_path
       FROM event evt
       JOIN  project prj ON prj.project_id = evt.project_id 
       WHERE evt.status = $eventStatusEncoder
         AND evt.execution_date < $executionDateEncoder
       ORDER BY evt.execution_date ASC
       LIMIT 1
       """
        .query(projectDecoder)
    ).arguments(AwaitingDeletion ~ executionDate)
      .build(_.option)
  }

  private def updateForProcessing(maybeProject: Option[Project]) =
    maybeProject map { case project @ Project(projectId, _) =>
      measureExecutionTime {
        SqlStatement(
          name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - update status")
        ).command[EventStatus ~ ExecutionDate ~ EventStatus ~ projects.Id](
          sql"""
           UPDATE event
           SET status = $eventStatusEncoder, execution_date = $executionDateEncoder
           WHERE status = $eventStatusEncoder
             AND project_id = $projectIdEncoder
       """.command
        ).arguments(Deleting ~ ExecutionDate(now()) ~ AwaitingDeletion ~ projectId)
          .build
          .mapResult {
            case Completion.Update(count) =>
              (CleanUpEvent(project) -> count).some
            case _ => Option.empty[(CleanUpEvent, Int)]
          }
      }
    } getOrElse Kleisli.pure(Option.empty[(CleanUpEvent, Int)])

  private def maybeUpdateMetrics(maybeCleanUpEvent: Option[(CleanUpEvent, Int)]) =
    maybeCleanUpEvent map { case (CleanUpEvent(Project(_, projectPath)), updatedRows) =>
      Kleisli.liftF {
        for {
          _ <- awaitingDeletionGauge.update((projectPath, updatedRows * -1))
          _ <- deletingGauge.update((projectPath, updatedRows))
        } yield ()
      }
    } getOrElse Kleisli.pure[F, Session[F], Unit](())
}

private object CleanUpEventFinder {
  def apply[F[_]: Async: Parallel: SessionResource](
      awatingDeletionGauge: LabeledGauge[F, projects.Path],
      deletingGauge:        LabeledGauge[F, projects.Path],
      queriesExecTimes:     LabeledHistogram[F, SqlStatement.Name]
  ): F[EventFinder[F, CleanUpEvent]] = MonadThrow[F].catchNonFatal {
    new CleanUpEventFinderImpl(awatingDeletionGauge, deletingGauge, queriesExecTimes)
  }
}
