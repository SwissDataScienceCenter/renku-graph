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
package projectsync

import cats.MonadThrow
import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events.LastSyncedDate
import io.renku.graph.model.projects
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private class EventFinderImpl[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes](
    now: () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with EventFinder[F, ProjectSyncEvent]
    with SubscriptionTypeSerializers {

  override def popEvent(): F[Option[ProjectSyncEvent]] = SessionResource[F].useK {
    findEvent >>= {
      case Some((projectId, maybeSyncedDate, event)) =>
        setSyncDate(projectId, maybeSyncedDate) map toNoneIfEventAlreadyTaken(event)
      case None => Kleisli.pure(Option.empty[ProjectSyncEvent])
    }
  }

  private def findEvent = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - find event")
      .select[LastSyncedDate, (projects.GitLabId, Option[LastSyncedDate], ProjectSyncEvent)](
        sql"""SELECT candidate.project_id, candidate.real_sync, candidate.project_slug
              FROM (
                SELECT proj.project_id, 
                  proj.project_slug,
                  sync_time.last_synced,
                  sync_time.last_synced AS real_sync
                FROM project proj
                JOIN subscription_category_sync_time sync_time
                  ON sync_time.project_id = proj.project_id AND sync_time.category_name = '#${categoryName.show}'
                WHERE ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 day' 
                UNION
                SELECT proj.project_id, 
                  proj.project_slug,
                  TIMESTAMP WITH TIME ZONE 'epoch' AS last_synced,
                  NULL AS real_sync
                FROM project proj
                LEFT JOIN subscription_category_sync_time sync_time
                  ON sync_time.project_id = proj.project_id AND sync_time.category_name = '#${categoryName.show}'
                WHERE sync_time IS NULL
              ) candidate
              ORDER BY candidate.last_synced ASC
              LIMIT 1
      """.query(projectIdDecoder ~ lastSyncedDateDecoder.opt ~ projectSlugDecoder)
          .map { case id ~ maybeDate ~ slug => (id, maybeDate, ProjectSyncEvent(id, slug)) }
      )
      .arguments(LastSyncedDate(now()))
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
      .command[LastSyncedDate *: projects.GitLabId *: EmptyTuple](sql"""
        UPDATE subscription_category_sync_time
        SET last_synced = $lastSyncedDateEncoder
        WHERE project_id = $projectIdEncoder AND category_name = '#${categoryName.show}'
        """.command)
      .arguments(LastSyncedDate(now()) *: projectId *: EmptyTuple)
      .build
      .flatMapResult {
        case Completion.Update(1) => true.pure[F]
        case Completion.Update(0) => false.pure[F]
        case completion =>
          new Exception(s"${categoryName.show}: update last_synced failed with completion code $completion")
            .raiseError[F, Boolean]
      }
  } recoverWith { case SqlState.ForeignKeyViolation(_) => Kleisli.pure(false) }

  private def insertLastSyncedDate(projectId: projects.GitLabId) = measureExecutionTime {
    SqlStatement
      .named(s"${categoryName.value.toLowerCase} - insert last_synced")
      .command[projects.GitLabId *: LastSyncedDate *: EmptyTuple](sql"""
        INSERT INTO subscription_category_sync_time(project_id, category_name, last_synced)
        VALUES ($projectIdEncoder, '#${categoryName.show}', $lastSyncedDateEncoder)
        ON CONFLICT (project_id, category_name)
        DO UPDATE SET last_synced = EXCLUDED.last_synced
        """.command)
      .arguments(projectId *: LastSyncedDate(now()) *: EmptyTuple)
      .build
      .flatMapResult {
        case Completion.Insert(1) => true.pure[F]
        case Completion.Insert(0) => false.pure[F]
        case completion =>
          new Exception(s"${categoryName.show}: insert last_synced failed with completion code $completion")
            .raiseError[F, Boolean]
      }
  } recoverWith { case SqlState.ForeignKeyViolation(_) => Kleisli.pure(false) }

  private def toNoneIfEventAlreadyTaken(event: ProjectSyncEvent): Boolean => Option[ProjectSyncEvent] = {
    case true  => Some(event)
    case false => None
  }

  private lazy val falseForForeignKeyViolation: PartialFunction[Throwable, Kleisli[F, Session[F], Boolean]] = {
    case SqlState.ForeignKeyViolation(_) => Kleisli.pure(false)
  }
}

private object EventFinder {
  def apply[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]: F[EventFinder[F, ProjectSyncEvent]] =
    MonadThrow[F].catchNonFatal(new EventFinderImpl[F]())
}
