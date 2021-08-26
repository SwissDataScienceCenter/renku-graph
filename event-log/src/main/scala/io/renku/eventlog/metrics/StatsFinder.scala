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

package io.renku.eventlog.metrics

import cats.data.NonEmptyList
import cats.effect.{BracketThrow, ContextShift, IO, Sync}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.db.implicits._
import ch.datascience.graph.model.events.{CategoryName, EventStatus, LastSyncedDate}
import ch.datascience.graph.model.projects.Path
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog._
import io.renku.eventlog.subscriptions.{SubscriptionTypeSerializers, commitsync, globalcommitsync, membersync}
import skunk._
import skunk.codec.all._
import skunk.implicits._

import java.time.Instant

trait StatsFinder[Interpretation[_]] {
  def countEventsByCategoryName(): Interpretation[Map[CategoryName, Long]]

  def statuses(): Interpretation[Map[EventStatus, Long]]

  def countEvents(statuses:   Set[EventStatus],
                  maybeLimit: Option[Int Refined Positive] = None
  ): Interpretation[Map[Path, Long]]
}

class StatsFinderImpl[Interpretation[_]: Sync: BracketThrow](
    sessionResource:  SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlStatement.Name],
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with StatsFinder[Interpretation]
    with SubscriptionTypeSerializers {

  override def countEventsByCategoryName(): Interpretation[Map[CategoryName, Long]] = sessionResource.useK {
    measureExecutionTime(countEventsPerCategoryName).map(_.toMap)
  }

  // format: off

  private def countEventsPerCategoryName = {
    val (eventDate, lastSyncedDate) = (EventDate.apply _ &&& LastSyncedDate.apply _) (now())
    SqlStatement(name = "category name events count").select[CategoryName ~ EventDate ~ LastSyncedDate ~ EventDate ~ LastSyncedDate ~ EventDate ~ LastSyncedDate ~
      CategoryName ~ CategoryName ~ CategoryName ~ EventDate ~ LastSyncedDate ~ EventDate ~ LastSyncedDate ~ CategoryName ~
      CategoryName ~ CategoryName  ~ LastSyncedDate ~ CategoryName ~ CategoryName ~ EventDate, (CategoryName, Long)](
      sql"""
          SELECT all_counts.category_name, SUM(all_counts.count)
          FROM (
            (
              SELECT sync_time.category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              JOIN subscription_category_sync_time sync_time
                ON sync_time.project_id = proj.project_id AND sync_time.category_name = $categoryNameEncoder
              WHERE
                   (($eventDateEncoder - proj.latest_event_date) < INTERVAL '1 hour' AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 minute')
                OR (($eventDateEncoder - proj.latest_event_date) < INTERVAL '1 day'  AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 hour')
                OR (($eventDateEncoder - proj.latest_event_date) > INTERVAL '1 day'  AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 day')
              GROUP BY sync_time.category_name
            ) UNION ALL (
              SELECT $categoryNameEncoder AS category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              WHERE proj.project_id NOT IN (
                SELECT project_id
                FROM subscription_category_sync_time
                WHERE category_name = $categoryNameEncoder
              )
            ) UNION ALL (
              SELECT sync_time.category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              JOIN subscription_category_sync_time sync_time
                ON sync_time.project_id = proj.project_id AND sync_time.category_name = $categoryNameEncoder
              WHERE
                   (($eventDateEncoder - proj.latest_event_date) <= INTERVAL '7 days' AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 hour')
                OR (($eventDateEncoder - proj.latest_event_date) >  INTERVAL '7 days' AND ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '1 day')
              GROUP BY sync_time.category_name
            )  UNION ALL (
              SELECT $categoryNameEncoder AS category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              WHERE proj.project_id NOT IN (
                SELECT project_id
                FROM subscription_category_sync_time
                WHERE category_name = $categoryNameEncoder
              ) 
            ) UNION ALL (
              SELECT sync_time.category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              LEFT JOIN subscription_category_sync_time sync_time
                ON proj.project_id = sync_time.project_id AND sync_time.category_name = $categoryNameEncoder
              WHERE ($lastSyncedDateEncoder - sync_time.last_synced) > INTERVAL '7 days'
              GROUP BY sync_time.category_name
            ) UNION ALL (
              SELECT $categoryNameEncoder AS category_name, COUNT(DISTINCT proj.project_id) AS count
              FROM project proj
              LEFT JOIN subscription_category_sync_time sync_time
                ON proj.project_id = sync_time.project_id AND sync_time.category_name = $categoryNameEncoder
              LEFT JOIN event evt
                ON proj.project_id = evt.project_id AND (($eventDateEncoder - evt.event_date) > INTERVAL '7 days')
              WHERE
                sync_time.last_synced IS NULL AND evt.event_id IS NOT NULL
            )
          ) all_counts
          GROUP BY all_counts.category_name
          """.query(categoryNameDecoder ~ numeric).map { case categoryName ~ (count: BigDecimal) => (categoryName, count.longValue) }
    ).arguments(membersync.categoryName ~ eventDate ~ lastSyncedDate ~ eventDate ~ lastSyncedDate ~ eventDate ~
      lastSyncedDate ~ membersync.categoryName ~ membersync.categoryName ~ commitsync.categoryName ~ eventDate ~
      lastSyncedDate ~ eventDate ~ lastSyncedDate ~ commitsync.categoryName ~ commitsync.categoryName ~ globalcommitsync.categoryName ~ lastSyncedDate ~ globalcommitsync.categoryName ~ globalcommitsync.categoryName ~ eventDate).build(_.toList)
  }

  override def statuses(): Interpretation[Map[EventStatus, Long]] = sessionResource.useK { 
    measureExecutionTime(findStatuses)
      .map(_.toMap)
      .map(addMissingStatues)
  }

  private lazy val findStatuses: SqlStatement[Interpretation, List[(EventStatus, Long)]] = SqlStatement(name = "statuses count").select[Void, (EventStatus, Long)](
        sql"""SELECT status, COUNT(event_id) FROM event GROUP BY status;"""
          .query(eventStatusDecoder ~ int8)
          .map { case status ~ count => (status, count) }
  ).arguments(Void).build(_.toList)


  private def addMissingStatues(stats: Map[EventStatus, Long]): Map[EventStatus, Long] =
    EventStatus.all.map(status => status -> stats.getOrElse(status, 0L)).toMap

  override def countEvents(statuses: Set[EventStatus],
                           maybeLimit: Option[Int Refined Positive] = None
                          ): Interpretation[Map[Path, Long]] =
    NonEmptyList.fromList(statuses.toList) match {
      case None => Map.empty[Path, Long].pure[Interpretation]
      case Some(statusesList) =>
        sessionResource.useK {
          measureExecutionTime(countProjectsEvents(statusesList, maybeLimit))
            .map(_.toMap)
        }
    }

  private def countProjectsEvents(statuses: NonEmptyList[EventStatus],
                                  maybeLimit: Option[Int Refined Positive] = None
                                 ) = maybeLimit match {
    case None => prepareQuery(statuses)
    case Some(limit) => prepareQuery(statuses, limit)
  }

  private def prepareQuery(statuses: NonEmptyList[EventStatus]) = SqlStatement(name = "projects events count")
    .select[Void, Path ~ Long]( sql"""SELECT
                                      project_path,
                                      (SELECT count(event_id) FROM event evt_int WHERE evt_int.project_id = prj.project_id AND status IN (#${statuses.toSql})) AS count
                                      FROM project prj
                                      WHERE EXISTS (
                                              SELECT project_id
                                              FROM event evt
                                              WHERE evt.project_id = prj.project_id AND status IN (#${statuses.toSql})
                                            )
              """.query(projectPathDecoder ~ int8).map { case path ~ count => (path, count) }
  ).arguments(Void).build(_.toList)

  private def prepareQuery(statuses: NonEmptyList[EventStatus], limit: Int Refined Positive) =
    SqlStatement(name = "projects events count limit").select[Int, (Path, Long)](
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
            .query(projectPathDecoder ~ int8)
            .map { case projectPath ~ count => (projectPath, count) }
    ).arguments(limit).build(_.toList)

  private implicit class StatusesOps(statuses: NonEmptyList[EventStatus]) {
    lazy val toSql: String = statuses.map(status => s"'$status'").mkString_(", ")
  }
}

object IOStatsFinder {

  def apply(
             sessionResource: SessionResource[IO, EventLogDB],
             queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name]
           )(implicit contextShift: ContextShift[IO]): IO[StatsFinder[IO]] = IO {
    new StatsFinderImpl(sessionResource, queriesExecTimes)
  }
}
