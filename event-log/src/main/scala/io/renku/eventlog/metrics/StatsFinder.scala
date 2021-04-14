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
import cats.effect.{Bracket, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.{CategoryName, EventStatus}
import ch.datascience.graph.model.projects.Path
import ch.datascience.metrics.LabeledHistogram
import doobie.implicits._
import doobie.util.fragment.Fragment
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog._
import io.renku.eventlog.subscriptions._

import java.time.Instant

trait StatsFinder[Interpretation[_]] {
  def countEventsByCategoryName(): Interpretation[Map[CategoryName, Long]]
  def statuses():                  Interpretation[Map[EventStatus, Long]]
  def countEvents(statuses:   Set[EventStatus],
                  maybeLimit: Option[Int Refined Positive] = None
  ): Interpretation[Map[Path, Long]]
}

class StatsFinderImpl(
    transactor:       DbTransactor[IO, EventLogDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
    now:              () => Instant = () => Instant.now
)(implicit ME:        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with StatsFinder[IO]
    with TypeSerializers {

  override def countEventsByCategoryName(): IO[Map[CategoryName, Long]] =
    measureExecutionTime(countEventsPerCategoryName)
      .transact(transactor.get)
      .map(_.toMap)

  private lazy val countEventsPerCategoryName = SqlQuery(
    sql"""|SELECT all_counts.category_name, SUM(all_counts.count)
          |FROM (
          |  (
          |    SELECT sync_time.category_name, COUNT(DISTINCT proj.project_id) AS count
          |    FROM project proj
          |    JOIN subscription_category_sync_time sync_time
          |      ON sync_time.project_id = proj.project_id AND sync_time.category_name = ${membersync.categoryName.value}
          |    WHERE
          |         ((${now()} - proj.latest_event_date) < INTERVAL '1 hour' AND (${now()} - sync_time.last_synced) > INTERVAL '1 minute')
          |      OR ((${now()} - proj.latest_event_date) < INTERVAL '1 day'  AND (${now()} - sync_time.last_synced) > INTERVAL '1 hour')
          |      OR ((${now()} - proj.latest_event_date) > INTERVAL '1 day'  AND (${now()} - sync_time.last_synced) > INTERVAL '1 day')
          |    GROUP BY sync_time.category_name
          |  ) UNION ALL (
          |    SELECT ${membersync.categoryName.value} AS category_name, COUNT(DISTINCT proj.project_id) AS count
          |    FROM project proj
          |    WHERE proj.project_id NOT IN (
          |      SELECT project_id 
          |      FROM subscription_category_sync_time 
          |      WHERE category_name = ${membersync.categoryName.value}
          |    )
          |  ) UNION ALL (
          |    SELECT sync_time.category_name, COUNT(DISTINCT proj.project_id) AS count
          |    FROM project proj
          |    JOIN subscription_category_sync_time sync_time
          |      ON sync_time.project_id = proj.project_id AND sync_time.category_name = ${commitsync.categoryName.value}
          |    WHERE
          |         ((${now()} - proj.latest_event_date) <= INTERVAL '7 days' AND (${now()} - sync_time.last_synced) > INTERVAL '1 hour')
          |      OR ((${now()} - proj.latest_event_date) >  INTERVAL '7 days' AND (${now()} - sync_time.last_synced) > INTERVAL '1 day')
          |    GROUP BY sync_time.category_name
          |  ) UNION ALL (
          |    SELECT ${commitsync.categoryName.value} AS category_name, COUNT(DISTINCT proj.project_id) AS count
          |    FROM project proj
          |    WHERE proj.project_id NOT IN (
          |      SELECT project_id 
          |      FROM subscription_category_sync_time 
          |      WHERE category_name = ${commitsync.categoryName.value}
          |    )
          |  )
          |) all_counts
          |GROUP BY all_counts.category_name
          |""".stripMargin
      .query[(CategoryName, Long)]
      .to[List],
    name = "category name events count"
  )

  override def statuses(): IO[Map[EventStatus, Long]] =
    measureExecutionTime(findStatuses)
      .transact(transactor.get)
      .map(_.toMap)
      .map(addMissingStatues)

  private lazy val findStatuses = SqlQuery(
    sql"""SELECT status, COUNT(event_id) FROM event GROUP BY status;""".stripMargin
      .query[(EventStatus, Long)]
      .to[List],
    name = "statuses count"
  )

  private def addMissingStatues(stats: Map[EventStatus, Long]): Map[EventStatus, Long] =
    EventStatus.all.map(status => status -> stats.getOrElse(status, 0L)).toMap

  override def countEvents(statuses:   Set[EventStatus],
                           maybeLimit: Option[Int Refined Positive] = None
  ): IO[Map[Path, Long]] =
    NonEmptyList.fromList(statuses.toList) match {
      case None => Map.empty[Path, Long].pure[IO]
      case Some(statusesList) =>
        measureExecutionTime(countProjectsEvents(statusesList, maybeLimit))
          .transact(transactor.get)
          .map(_.toMap)
    }

  private def countProjectsEvents(statuses:   NonEmptyList[EventStatus],
                                  maybeLimit: Option[Int Refined Positive] = None
  ) = maybeLimit match {
    case None        => prepareQuery(statuses)
    case Some(limit) => prepareQuery(statuses, limit)
  }

  private def prepareQuery(statuses: NonEmptyList[EventStatus]) = SqlQuery(
    query = Fragment
      .const {
        s"""|SELECT
            |  project_path,
            |  (SELECT count(event_id) FROM event evt_int WHERE evt_int.project_id = prj.project_id AND status IN (${statuses.toSql})) AS count
            |FROM project prj
            |WHERE EXISTS (
            |        SELECT project_id
            |        FROM event evt
            |        WHERE evt.project_id = prj.project_id AND status IN (${statuses.toSql})
            |      )
            |""".stripMargin
      }
      .query[(Path, Long)]
      .to[List],
    name = "projects events count"
  )

  private def prepareQuery(statuses: NonEmptyList[EventStatus], limit: Int Refined Positive) = SqlQuery(
    query = Fragment
      .const {
        s"""|SELECT
            |  project_path,
            |  (select count(event_id) FROM event evt_int WHERE evt_int.project_id = prj.project_id AND status IN (${statuses.toSql})) AS count
            |FROM (select project_id, project_path, latest_event_date
            |      FROM project
            |      ORDER BY latest_event_date desc) prj
            |WHERE EXISTS (
            |        SELECT project_id
            |        FROM event evt
            |        WHERE evt.project_id = prj.project_id AND status IN (${statuses.toSql})
            |      )
            |LIMIT $limit;
            |""".stripMargin
      }
      .query[(Path, Long)]
      .to[List],
    name = "projects events count limit"
  )

  private implicit class StatusesOps(statuses: NonEmptyList[EventStatus]) {
    lazy val toSql: String = statuses.map(status => s"'$status'").mkString_(", ")
  }
}

object IOStatsFinder {

  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[StatsFinder[IO]] = IO {
    new StatsFinderImpl(transactor, queriesExecTimes)
  }
}
