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

package io.renku.eventlog.events.producers
package cleanup

import cats.data.Kleisli
import cats.data.Kleisli.liftF
import cats.effect.Async
import cats.syntax.all._
import cats.{MonadThrow, Parallel}
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes}
import io.renku.events.consumers.Project
import io.renku.graph.model.events.EventStatus.{AwaitingDeletion, Deleting}
import io.renku.graph.model.events.ExecutionDate
import io.renku.graph.model.projects
import org.typelevel.log4cats.Logger
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private class EventFinderImpl[F[_]: Async: Parallel: SessionResource: Logger: QueriesExecutionTimes: EventStatusGauges](
    now: () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with EventFinder[F, CleanUpEvent]
    with SubscriptionTypeSerializers
    with TypeSerializers {

  private type EventAndUpdatedCount = (CleanUpEvent, Int)

  override def popEvent(): F[Option[CleanUpEvent]] = SessionResource[F].useWithTransactionK {
    Kleisli { case (transaction, session) =>
      liftF(transaction.savepoint)
        .flatMap { savepoint =>
          findEventAndMarkTaken
            .flatTap(commit(transaction))
            .handleErrorWith(rollback(transaction)(savepoint)(_).as(Option.empty[EventAndUpdatedCount]))
            .flatTap(updateMetrics)
            .map(_.map { case (cleanUpEvent, _) => cleanUpEvent })
        }
        .run(session)
    }
  }

  private def findEventAndMarkTaken =
    findInCleanUpEvents >>= {
      case Some(event) => Kleisli.pure(Option(event))
      case None        => findInEvents
    } >>= removeEventFromQueue() >>= markEventsDeleting()

  private def findInCleanUpEvents = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.show.toLowerCase} - find event in queue")
      .select[Void, Project](sql"""
        SELECT queue.project_id, queue.project_path
        FROM clean_up_events_queue queue
        ORDER BY queue.date ASC
        LIMIT 1
        """.query(projectDecoder))
      .arguments(Void)
      .build(_.option)
  }

  private def findInEvents = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.show.toLowerCase} - find event")
      .select[ExecutionDate, Project](sql"""
        SELECT evt.project_id, prj.project_path
        FROM event evt
        JOIN  project prj ON prj.project_id = evt.project_id 
        WHERE evt.status = '#${AwaitingDeletion.value}'
          AND evt.execution_date <= $executionDateEncoder
        ORDER BY evt.execution_date ASC
        LIMIT 1
        """.query(projectDecoder))
      .arguments(ExecutionDate(now()))
      .build(_.option)
  }

  private def removeEventFromQueue(): Option[Project] => Kleisli[F, Session[F], Option[Project]] = {
    case Some(project @ Project(_, projectPath)) =>
      measureExecutionTime {
        SqlStatement
          .named(s"${categoryName.show.toLowerCase} - delete clean-up event")
          .command[projects.Path](sql"""
            DELETE FROM clean_up_events_queue
            WHERE project_path = $projectPathEncoder
            """.command)
          .arguments(projectPath)
          .build
          .mapResult {
            case Completion.Delete(_) => project.some
            case _                    => Option.empty[Project]
          }
      }
    case None => Kleisli.pure(Option.empty[Project])
  }

  private def markEventsDeleting(): Option[Project] => Kleisli[F, Session[F], Option[EventAndUpdatedCount]] = {
    case Some(project @ Project(projectId, _)) =>
      measureExecutionTime {
        SqlStatement
          .named(s"${categoryName.show.toLowerCase} - update status")
          .command[ExecutionDate *: projects.GitLabId *: EmptyTuple](sql"""
            UPDATE event
            SET status = '#${Deleting.value}', execution_date = $executionDateEncoder
            WHERE status = '#${AwaitingDeletion.value}'
              AND project_id = $projectIdEncoder
            """.command)
          .arguments(ExecutionDate(now()) *: projectId *: EmptyTuple)
          .build
          .mapResult {
            case Completion.Update(count) => (CleanUpEvent(project) -> count).some
            case _                        => Option.empty[EventAndUpdatedCount]
          }
      }
    case None => Kleisli.pure(Option.empty[EventAndUpdatedCount])
  }

  private lazy val updateMetrics: Option[EventAndUpdatedCount] => Kleisli[F, Session[F], Unit] = {
    case Some(CleanUpEvent(Project(_, projectPath)) -> updatedRows) =>
      Kleisli.liftF {
        EventStatusGauges[F].awaitingDeletion.update((projectPath, updatedRows * -1)) >>
          EventStatusGauges[F].underDeletion.update((projectPath, updatedRows))
      }
    case None => Kleisli.pure[F, Session[F], Unit](())
  }

  private def commit(transaction: Transaction[F]): Option[EventAndUpdatedCount] => Kleisli[F, Session[F], Unit] = _ =>
    Kleisli.liftF(transaction.commit.void)

  private def rollback(
      transaction: Transaction[F]
  )(savepoint: transaction.Savepoint): Throwable => Kleisli[F, Session[F], Unit] = { err =>
    Kleisli.liftF {
      transaction.rollback(savepoint) >>
        Logger[F].info(err)(show"$categoryName: problems with DB update; rollback")
    }
  }
}

private object EventFinder {
  def apply[F[_]: Async: Parallel: SessionResource: Logger: QueriesExecutionTimes: EventStatusGauges]
      : F[EventFinder[F, CleanUpEvent]] = MonadThrow[F].catchNonFatal {
    new EventFinderImpl[F]()
  }
}
