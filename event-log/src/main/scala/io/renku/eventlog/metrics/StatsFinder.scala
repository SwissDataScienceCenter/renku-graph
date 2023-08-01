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

package io.renku.eventlog.metrics

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.db.implicits._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.events.producers._
import io.renku.events.CategoryName
import io.renku.graph.model.events.EventStatus.TriplesStore
import io.renku.graph.model.events.{EventDate, EventStatus, LastSyncedDate}
import io.renku.graph.model.projects.Slug
import skunk._
import skunk.codec.all._
import skunk.implicits._

import java.time.Instant

trait StatsFinder[F[_]] {

  def countEventsByCategoryName(): F[Map[CategoryName, Long]]

  def statuses(): F[Map[EventStatus, Long]]

  def countEvents(statuses: Set[EventStatus], maybeLimit: Option[Int Refined Positive] = None): F[Map[Slug, Long]]
}

class StatsFinderImpl[F[_]: Async: SessionResource: QueriesExecutionTimes](
    now: () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with StatsFinder[F]
    with SubscriptionTypeSerializers {

  override def countEventsByCategoryName(): F[Map[CategoryName, Long]] = SessionResource[F].useK {
    countEventsPerCategoryName.map(_.toMap)
  }

  private def countEventsPerCategoryName = measureExecutionTime {
    val (eventDate, lastSyncedDate) = (EventDate.apply _ &&& LastSyncedDate.apply)(now())
    SqlStatement(name = "category name events count")
      .select[
        EventDate *: LastSyncedDate *: EventDate *: LastSyncedDate *: EventDate *: LastSyncedDate *: // MEMBER_SYNC
          EventDate *: LastSyncedDate *: EventDate *: LastSyncedDate *: LastSyncedDate *: // COMMIT_SYNC
          LastSyncedDate *: // PROJECT_SYNC
          EmptyTuple,
        (CategoryName, Long)
      ](
        sql"""
          SELECT all_counts.category_name, SUM(all_counts.count)
          FROM (
            -- MEMBER_SYNC
            (
              SELECT sync_time.category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              JOIN subscription_category_sync_time sync_time
                ON sync_time.project_id = proj.project_id AND sync_time.category_name = '#${membersync.categoryName.show}'
              WHERE
                   (($eventDateEncoder - proj.latest_event_date) < INTERVAL '1 hour' AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '5 minutes')
                OR (($eventDateEncoder - proj.latest_event_date) < INTERVAL '1 day'  AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 hour')
                OR (($eventDateEncoder - proj.latest_event_date) > INTERVAL '1 day'  AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 day')
              GROUP BY sync_time.category_name
            ) UNION ALL (
              SELECT '#${membersync.categoryName.show}' AS category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              WHERE proj.project_id NOT IN (
                SELECT project_id
                FROM subscription_category_sync_time
                WHERE category_name = '#${membersync.categoryName.show}'
              )
            -- COMMIT_SYNC
            ) UNION ALL (
              SELECT sync_time.category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              JOIN subscription_category_sync_time sync_time
                ON sync_time.project_id = proj.project_id AND sync_time.category_name = '#${commitsync.categoryName.show}'
              WHERE
                   (($eventDateEncoder - proj.latest_event_date) <= INTERVAL '7 days' AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 hour')
                OR (($eventDateEncoder - proj.latest_event_date) >  INTERVAL '7 days' AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 day')
              GROUP BY sync_time.category_name
            ) UNION ALL (
              SELECT '#${commitsync.categoryName.show}' AS category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              WHERE proj.project_id NOT IN (
                SELECT project_id
                FROM subscription_category_sync_time
                WHERE category_name = '#${commitsync.categoryName.show}'
              ) 
            -- GLOBAL_COMMIT_SYNC
            ) UNION ALL (
              SELECT sync_time.category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              LEFT JOIN subscription_category_sync_time sync_time
                ON proj.project_id = sync_time.project_id AND sync_time.category_name = '#${globalcommitsync.categoryName.show}'
              WHERE ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '7 days'
              GROUP BY sync_time.category_name
            ) UNION ALL (
              SELECT '#${globalcommitsync.categoryName.show}' AS category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              WHERE proj.project_id NOT IN (
                SELECT project_id
                FROM subscription_category_sync_time
                WHERE category_name = '#${globalcommitsync.categoryName.show}'
              ) 
            -- PROJECT_SYNC  
            ) UNION ALL (
              SELECT sync_time.category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              LEFT JOIN subscription_category_sync_time sync_time
                ON proj.project_id = sync_time.project_id AND sync_time.category_name = '#${projectsync.categoryName.show}'
              WHERE ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 day'
              GROUP BY sync_time.category_name
            ) UNION ALL (
              SELECT '#${projectsync.categoryName.show}' AS category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              WHERE proj.project_id NOT IN (
                SELECT project_id
                FROM subscription_category_sync_time
                WHERE category_name = '#${projectsync.categoryName.show}'
              ) 
            -- MIN_PROJECT_INFO
            ) UNION ALL (
              SELECT '#${minprojectinfo.categoryName.show}' AS category_name, COUNT(DISTINCT p.project_id) AS count
              FROM project p
              WHERE NOT EXISTS (
                SELECT project_id 
                FROM subscription_category_sync_time st 
                WHERE st.category_name = '#${minprojectinfo.categoryName.show}' 
                  AND st.project_id = p.project_id
              ) AND NOT EXISTS (
                SELECT event_id 
                FROM event e
                WHERE e.project_id = p.project_id
                  AND e.status = '#${TriplesStore.value}'
              )
            ) UNION ALL (
              SELECT event_type AS category_name, COUNT(DISTINCT id) AS count
              FROM status_change_events_queue
              GROUP BY event_type
            ) UNION ALL (
              SELECT 'CLEAN_UP_EVENT' AS category_name, COUNT(DISTINCT id) AS count
              FROM clean_up_events_queue
            )
          ) all_counts
          GROUP BY all_counts.category_name
          """.query(categoryNameDecoder ~ numeric).map { case categoryName ~ (count: BigDecimal) =>
          (categoryName, count.longValue)
        }
      )
      .arguments(
        eventDate *: lastSyncedDate *: eventDate *: lastSyncedDate *: eventDate *: lastSyncedDate *: // MEMBER_SYNC
          eventDate *: lastSyncedDate *: eventDate *: lastSyncedDate *: lastSyncedDate *: // COMMIT_SYNC
          lastSyncedDate *: // PROJECT_SYNC
          EmptyTuple
      )
      .build(_.toList)
  }

  override def statuses(): F[Map[EventStatus, Long]] = SessionResource[F].useK {
    measureExecutionTime(findStatuses)
      .map(_.toMap)
      .map(addMissingStatues)
  }

  private lazy val findStatuses: SqlStatement[F, List[(EventStatus, Long)]] = SqlStatement(name = "statuses count")
    .select[Void, (EventStatus, Long)](
      sql"""SELECT status, COUNT(event_id) FROM event GROUP BY status;"""
        .query(eventStatusDecoder ~ int8)
        .map { case status ~ count => (status, count) }
    )
    .arguments(Void)
    .build(_.toList)

  private def addMissingStatues(stats: Map[EventStatus, Long]): Map[EventStatus, Long] =
    EventStatus.all.map(status => status -> stats.getOrElse(status, 0L)).toMap

  override def countEvents(statuses:   Set[EventStatus],
                           maybeLimit: Option[Int Refined Positive] = None
  ): F[Map[Slug, Long]] =
    NonEmptyList.fromList(statuses.toList) match {
      case None => Map.empty[Slug, Long].pure[F]
      case Some(statusesList) =>
        SessionResource[F].useK {
          measureExecutionTime(countProjectsEvents(statusesList, maybeLimit))
            .map(_.toMap)
        }
    }

  private def countProjectsEvents(statuses:   NonEmptyList[EventStatus],
                                  maybeLimit: Option[Int Refined Positive] = None
  ) = maybeLimit match {
    case None        => prepareQuery(statuses)
    case Some(limit) => prepareQuery(statuses, limit)
  }

  private def prepareQuery(statuses: NonEmptyList[EventStatus]) =
    SqlStatement(name = "projects events count")
      .select[Void, Slug ~ Long](sql"""SELECT
                                      project_path,
                                      (SELECT count(event_id) FROM event evt_int WHERE evt_int.project_id = prj.project_id AND status IN (#${statuses.toSql})) AS count
                                      FROM project prj
                                      WHERE EXISTS (
                                              SELECT project_id
                                              FROM event evt
                                              WHERE evt.project_id = prj.project_id AND status IN (#${statuses.toSql})
                                            )
              """.query(projectSlugDecoder ~ int8).map { case slug ~ count => (slug, count) })
      .arguments(Void)
      .build(_.toList)

  private def prepareQuery(statuses: NonEmptyList[EventStatus], limit: Int Refined Positive) =
    SqlStatement(name = "projects events count limit")
      .select[Int, (Slug, Long)](
        sql"""
      SELECT
        project_path,
        (select count(event_id) FROM event evt_int WHERE evt_int.project_id = prj.project_id AND status IN (#${statuses.toSql})) AS count
      FROM (select project_id, project_path, latest_event_date
            FROM project
            ORDER BY latest_event_date desc) prj
      WHERE EXISTS (
              SELECT project_id
              FROM event evt
              WHERE evt.project_id = prj.project_id AND status IN (#${statuses.toSql})
            )
      LIMIT $int4;
      """
          .query(projectSlugDecoder ~ int8)
          .map { case projectSlug ~ count => (projectSlug, count) }
      )
      .arguments(limit)
      .build(_.toList)

  private implicit class StatusesOps(statuses: NonEmptyList[EventStatus]) {
    lazy val toSql: String = statuses.map(status => s"'$status'").mkString_(", ")
  }
}

object StatsFinder {
  def apply[F[_]: MonadThrow: Async: SessionResource: QueriesExecutionTimes]: F[StatsFinder[F]] =
    MonadThrow[F].catchNonFatal(new StatsFinderImpl[F]())
}
