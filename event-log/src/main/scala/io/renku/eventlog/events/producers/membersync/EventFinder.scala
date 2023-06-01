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

package io.renku.eventlog.events.producers.membersync

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.producers.{EventFinder, SubscriptionTypeSerializers}
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.CategoryName
import io.renku.graph.model.events.{EventDate, LastSyncedDate}
import io.renku.graph.model.projects
import skunk.data.Completion
import skunk.implicits._
import skunk._

import java.time.Instant

private class EventFinderImpl[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes](
    now: () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with EventFinder[F, MemberSyncEvent]
    with SubscriptionTypeSerializers {

  override def popEvent(): F[Option[MemberSyncEvent]] = SessionResource[F].useK {
    findEventAndMarkTaken()
  }

  private def findEventAndMarkTaken() = findEvent >>= {
    case Some((projectId, maybeSyncedDate, event)) =>
      setSyncDate(projectId, maybeSyncedDate) map toNoneIfEventAlreadyTaken(event)
    case None => Kleisli.pure(Option.empty[MemberSyncEvent])
  }

  private def findEvent = measureExecutionTime {
    val eventDate    = EventDate(now())
    val lastSyncDate = LastSyncedDate(now())
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - find event")
      .select[
        CategoryName *: EventDate *: LastSyncedDate *: EventDate *: LastSyncedDate *: EventDate *: LastSyncedDate *: EmptyTuple,
        (projects.GitLabId, Option[LastSyncedDate], MemberSyncEvent)
      ](
        sql"""SELECT proj.project_id, sync_time.last_synced, proj.project_path
              FROM project proj
              LEFT JOIN subscription_category_sync_time sync_time
                ON sync_time.project_id = proj.project_id AND sync_time.category_name = $categoryNameEncoder
              WHERE
                sync_time.last_synced IS NULL
                OR (
                     (($eventDateEncoder - proj.latest_event_date) < INTERVAL '1 hour' AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '5 minutes')
                  OR (($eventDateEncoder - proj.latest_event_date) < INTERVAL '1 day'  AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 hour')
                  OR (($eventDateEncoder - proj.latest_event_date) > INTERVAL '1 day'  AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 day')
                )
              ORDER BY proj.latest_event_date DESC
              LIMIT 1
      """.query(projectIdDecoder ~ lastSyncedDateDecoder.opt ~ projectPathDecoder)
          .map { case id ~ maybeDate ~ path => (id, maybeDate, MemberSyncEvent(path)) }
      )
      .arguments(
        categoryName *: eventDate *: lastSyncDate *: eventDate *: lastSyncDate *: eventDate *: lastSyncDate *: EmptyTuple
      )
      .build(_.option)
  }

  private def setSyncDate(projectId:       projects.GitLabId,
                          maybeSyncedDate: Option[LastSyncedDate]
  ): Kleisli[F, Session[F], Boolean] = {
    if (maybeSyncedDate.isDefined) updateLastSyncedDate(projectId)
    else insertLastSyncedDate(projectId)
  } recoverWith falseForForeignKeyViolation

  private def updateLastSyncedDate(projectId: projects.GitLabId) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - update last_synced")
      .command[LastSyncedDate *: projects.GitLabId *: CategoryName *: EmptyTuple](sql"""
        UPDATE subscription_category_sync_time
        SET last_synced = $lastSyncedDateEncoder
        WHERE project_id = $projectIdEncoder AND category_name = $categoryNameEncoder
        """.command)
      .arguments(LastSyncedDate(now()) *: projectId *: categoryName *: EmptyTuple)
      .build
      .flatMapResult {
        case Completion.Update(1) => true.pure[F]
        case Completion.Update(0) => false.pure[F]
        case completion =>
          new Exception(s"${categoryName.show}: update last_synced failed with completion code $completion")
            .raiseError[F, Boolean]
      }
  }

  private def insertLastSyncedDate(projectId: projects.GitLabId) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - insert last_synced")
      .command[projects.GitLabId *: CategoryName *: LastSyncedDate *: EmptyTuple](sql"""
        INSERT INTO subscription_category_sync_time(project_id, category_name, last_synced)
        VALUES ($projectIdEncoder, $categoryNameEncoder, $lastSyncedDateEncoder)
        ON CONFLICT (project_id, category_name)
        DO UPDATE SET last_synced = EXCLUDED.last_synced
        """.command)
      .arguments(projectId *: categoryName *: LastSyncedDate(now()) *: EmptyTuple)
      .build
      .flatMapResult {
        case Completion.Insert(1) => true.pure[F]
        case Completion.Insert(0) => false.pure[F]
        case completion =>
          new Exception(s"${categoryName.show}: insert last_synced failed with completion code $completion")
            .raiseError[F, Boolean]
      }
  }

  private def toNoneIfEventAlreadyTaken(event: MemberSyncEvent): Boolean => Option[MemberSyncEvent] = {
    case true  => Some(event)
    case false => None
  }

  private lazy val falseForForeignKeyViolation: PartialFunction[Throwable, Kleisli[F, Session[F], Boolean]] = {
    case SqlState.ForeignKeyViolation(_) => Kleisli.pure(false)
  }
}

private object EventFinder {
  def apply[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]: F[EventFinder[F, MemberSyncEvent]] =
    MonadThrow[F].catchNonFatal(new EventFinderImpl[F]())
}
