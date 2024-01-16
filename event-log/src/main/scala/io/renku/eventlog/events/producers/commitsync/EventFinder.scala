/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
package commitsync

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.CategoryName
import io.renku.events.consumers.Project
import io.renku.graph.model.events.EventStatus.AwaitingDeletion
import io.renku.graph.model.events.{CompoundEventId, EventDate, EventId, EventStatus, LastSyncedDate}
import io.renku.graph.model.projects
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private class EventFinderImpl[F[_]: Async: SessionResource: QueriesExecutionTimes](
    now: () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with EventFinder[F, CommitSyncEvent]
    with SubscriptionTypeSerializers {

  override def popEvent(): F[Option[CommitSyncEvent]] = SessionResource[F].useK(findEventAndMarkTaken)

  private def findEventAndMarkTaken = findEvent >>= {
    case Some((event, maybeSyncDate, Some(eventStatus))) if eventStatus == AwaitingDeletion =>
      setSyncDate(event, maybeSyncDate).as(Option.empty[CommitSyncEvent])
    case Some((event, maybeSyncDate, _)) =>
      setSyncDate(event, maybeSyncDate) map toNoneIfEventAlreadyTaken(event)
    case None => Kleisli.pure(Option.empty[CommitSyncEvent])
  }

  private def findEvent = measureExecutionTime {
    val (eventDate, lastSyncDate) = (EventDate.apply _ &&& LastSyncedDate.apply)(now())
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - find event")
      .select[CategoryName *: EventDate *: LastSyncedDate *: EventDate *: LastSyncedDate *: EmptyTuple,
              (CommitSyncEvent, Option[LastSyncedDate], Option[EventStatus])
      ](
        sql"""
          SELECT
            (SELECT evt.event_id
              FROM event evt
              WHERE evt.project_id = proj.project_id
                AND evt.event_date = proj.latest_event_date
              ORDER BY created_date DESC
              LIMIT 1
            ) event_id,
		      (SELECT evt.status
              FROM event evt
              WHERE evt.project_id = proj.project_id
                AND evt.event_date = proj.latest_event_date
              ORDER BY created_date DESC
              LIMIT 1
            ) event_status,
            proj.project_id,
            proj.project_slug,
            sync_time.last_synced,
            proj.latest_event_date
          FROM project proj
          LEFT JOIN subscription_category_sync_time sync_time
            ON sync_time.project_id = proj.project_id AND sync_time.category_name = $categoryNameEncoder
          WHERE
           (sync_time.last_synced IS NULL
            OR (
                 (($eventDateEncoder - proj.latest_event_date) <= INTERVAL '7 days' AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 hour')
              OR (($eventDateEncoder - proj.latest_event_date) >  INTERVAL '7 days' AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 day')
            ))
          ORDER BY proj.latest_event_date DESC
          LIMIT 1
          """
          .query(
            eventIdDecoder.opt ~ eventStatusDecoder.opt ~ projectDecoder ~ lastSyncedDateDecoder.opt ~ eventDateDecoder
          )
          .map {
            case Some(eventId: EventId) ~
                (maybeEventStatus: Option[EventStatus]) ~
                (project: Project) ~ (maybeLastSyncDate: Option[LastSyncedDate]) ~ (latestEventDate: EventDate) =>
              (FullCommitSyncEvent(CompoundEventId(eventId, project.id),
                                   project.slug,
                                   maybeLastSyncDate getOrElse LastSyncedDate(latestEventDate.value)
               ),
               maybeLastSyncDate,
               maybeEventStatus
              )
            case None ~ _ ~ (project: Project) ~ (maybeLastSyncDate: Option[LastSyncedDate]) ~ _ =>
              (MinimalCommitSyncEvent(project), maybeLastSyncDate, None)
          }
      )
      .arguments(categoryName *: eventDate *: lastSyncDate *: eventDate *: lastSyncDate *: EmptyTuple)
      .build(_.option)
      .mapResult {
        case Some((event: FullCommitSyncEvent, maybeSyncDate, maybeEventStatus)) =>
          Some((event, maybeSyncDate, maybeEventStatus))
        case Some((event: MinimalCommitSyncEvent, maybeSyncDate, maybeEventStatus)) =>
          Some((event, maybeSyncDate, maybeEventStatus))
        case _ => None
      }
  }

  private def setSyncDate(event: CommitSyncEvent, maybeSyncedDate: Option[LastSyncedDate]) =
    if (maybeSyncedDate.isDefined) updateLastSyncedDate(event)
    else insertLastSyncedDate(event)

  private def updateLastSyncedDate(event: CommitSyncEvent) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - update last_synced")
      .command[LastSyncedDate *: projects.GitLabId *: CategoryName *: EmptyTuple](sql"""
        UPDATE subscription_category_sync_time
        SET last_synced = $lastSyncedDateEncoder
        WHERE project_id = $projectIdEncoder AND category_name = $categoryNameEncoder
        """.command)
      .arguments(LastSyncedDate(now()) *: event.projectId *: categoryName *: EmptyTuple)
      .build
  } recoverWith { case SqlState.ForeignKeyViolation(_) => Kleisli.pure(Completion.Insert(0)) }

  private def insertLastSyncedDate(event: CommitSyncEvent) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - insert last_synced")
      .command[projects.GitLabId *: CategoryName *: LastSyncedDate *: EmptyTuple](sql"""
        INSERT INTO subscription_category_sync_time(project_id, category_name, last_synced)
        VALUES ($projectIdEncoder, $categoryNameEncoder, $lastSyncedDateEncoder)
        ON CONFLICT (project_id, category_name)
        DO UPDATE SET last_synced = EXCLUDED.last_synced
        """.command)
      .arguments(event.projectId *: categoryName *: LastSyncedDate(now()) *: EmptyTuple)
      .build
  } recoverWith { case SqlState.ForeignKeyViolation(_) => Kleisli.pure(Completion.Insert(0)) }

  private implicit class SyncEventOps(commitSyncEvent: CommitSyncEvent) {
    lazy val projectId: projects.GitLabId = commitSyncEvent match {
      case FullCommitSyncEvent(eventId, _, _) => eventId.projectId
      case MinimalCommitSyncEvent(project)    => project.id
    }
  }

  private def toNoneIfEventAlreadyTaken(event: CommitSyncEvent): Completion => Option[CommitSyncEvent] = {
    case Completion.Update(1) | Completion.Insert(1) => Some(event)
    case _                                           => None
  }
}

private object EventFinder {
  def apply[F[_]: Async: SessionResource: QueriesExecutionTimes]: F[EventFinder[F, CommitSyncEvent]] =
    MonadThrow[F].catchNonFatal(new EventFinderImpl[F]())
}
