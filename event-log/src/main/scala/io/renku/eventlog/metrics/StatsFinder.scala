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

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.{Async, Bracket, ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events.{CategoryName, EventStatus}
import ch.datascience.graph.model.projects.Path
import ch.datascience.metrics.LabeledHistogram
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog._
import io.renku.eventlog.subscriptions.{LastSyncedDate, SubscriptionTypeSerializers}
import skunk._
import skunk.implicits._
import skunk.codec.all._

import java.time.Instant

trait StatsFinder[Interpretation[_]] {
  def countEventsByCategoryName(): Interpretation[Map[CategoryName, Long]]
  def statuses():                  Interpretation[Map[EventStatus, Long]]
  def countEvents(statuses:   Set[EventStatus],
                  maybeLimit: Option[Int Refined Positive] = None
  ): Interpretation[Map[Path, Long]]
}

class StatsFinderImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    transactor:       SessionResource[Interpretation, EventLogDB],
    queriesExecTimes: LabeledHistogram[Interpretation, SqlQuery.Name],
    categoryNames:    Set[CategoryName] Refined NonEmpty,
    now:              () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with StatsFinder[Interpretation]
    with SubscriptionTypeSerializers {

  override def countEventsByCategoryName(): Interpretation[Map[CategoryName, Long]] = transactor.use {
    implicit session =>
      measureExecutionTime(countEventsPerCategoryName).map(_.toMap)
  }

  // format: off
  
  private lazy val countEventsPerCategoryName = SqlQuery[Interpretation, List[(CategoryName, Long)]](
    Kleisli{ session =>
      val query : Query[EventDate ~ LastSyncedDate ~ EventDate ~ LastSyncedDate ~ EventDate ~ LastSyncedDate, (CategoryName, Long)] = sql"""SELECT all_counts.category_name, SUM(all_counts.count)
          FROM (
            (
              SELECT sync_time.category_name, COUNT(DISTINCT proj.project_id)
              FROM project proj
              JOIN subscription_category_sync_time sync_time ON sync_time.project_id = proj.project_id
              WHERE
                   (($eventDatePut - proj.latest_event_date) < INTERVAL '1 hour' AND ($lastSyncedDatePut - sync_time.last_synced) > INTERVAL '1 minute')
                OR (($eventDatePut - proj.latest_event_date) < INTERVAL '1 day'  AND ($lastSyncedDatePut - sync_time.last_synced) > INTERVAL '1 hour')
                OR (($eventDatePut - proj.latest_event_date) > INTERVAL '1 day'  AND ($lastSyncedDatePut - sync_time.last_synced) > INTERVAL '1 day')
              GROUP BY sync_time.category_name
            ) UNION #${(categoryNames map generateUnions).reduce(_ ++ " UNION " ++ _)}
          ) all_counts
          GROUP BY all_counts.category_name;
          """.query(categoryNameGet ~ numeric).map{ case categoryName ~ (count:BigDecimal) => (categoryName, count.longValue)}
      val eventDate = EventDate(now())
      val lastSyncedDate = LastSyncedDate(now())
      session.prepare(query).use{_.stream(eventDate ~lastSyncedDate ~ eventDate ~lastSyncedDate ~ eventDate ~lastSyncedDate, 32).compile.toList}
    },
    name = "category name events count"
  )
  // format: on
  // TODO could we make it safer with sql string interpolation
  private def generateUnions(categoryName: CategoryName) =
    s"""  (
            SELECT '$categoryName' AS category_name, COUNT(DISTINCT proj.project_id)
            FROM project proj
            WHERE
              proj.project_id NOT IN (
                SELECT project_id from subscription_category_sync_time WHERE category_name = '$categoryName'
              )
          )
    """

  override def statuses(): Interpretation[Map[EventStatus, Long]] = transactor.use { implicit session =>
    measureExecutionTime(findStatuses)
      .map(_.toMap)
      .map(addMissingStatues)
  }

  private lazy val findStatuses: SqlQuery[Interpretation, List[(EventStatus, Long)]] = SqlQuery(
    Kleisli { session =>
      val query: Query[Void, (EventStatus, Long)] = sql"""SELECT status, COUNT(event_id) FROM event GROUP BY status;"""
        .query(eventStatusGet ~ int8)
        .map { case status ~ count => (status, count) }
      session.execute(query)
    },
    name = "statuses count"
  )

  private def addMissingStatues(stats: Map[EventStatus, Long]): Map[EventStatus, Long] =
    EventStatus.all.map(status => status -> stats.getOrElse(status, 0L)).toMap

  override def countEvents(statuses:   Set[EventStatus],
                           maybeLimit: Option[Int Refined Positive] = None
  ): Interpretation[Map[Path, Long]] =
    NonEmptyList.fromList(statuses.toList) match {
      case None => Map.empty[Path, Long].pure[Interpretation]
      case Some(statusesList) =>
        transactor.use { implicit session =>
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

  private def prepareQuery(statuses: NonEmptyList[EventStatus]) = SqlQuery[Interpretation, List[(Path, Long)]](
    Kleisli { session =>
      val query: Query[Void, Path ~ Long] = sql"""SELECT
                project_path,
                (SELECT count(event_id) FROM event evt_int WHERE evt_int.project_id = prj.project_id AND status IN (#${statuses.toSql})) AS count
              FROM project prj
              WHERE EXISTS (
                      SELECT project_id
                      FROM event evt
                      WHERE evt.project_id = prj.project_id AND status IN (#${statuses.toSql})
                    )
              """.query(projectPathGet ~ int8).map { case path ~ count => (path, count) }
      session.execute(query)
    },
    name = "projects events count"
  )

  private def prepareQuery(statuses: NonEmptyList[EventStatus], limit: Int Refined Positive) =
    SqlQuery[Interpretation, List[(Path, Long)]](
      Kleisli { session =>
        val query: Query[Int, (Path, Long)] = sql"""
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
          .query(projectPathGet ~ int8)
          .map { case projectPath ~ count => (projectPath, count) }
        session.prepare(query).use(_.stream(limit, 32).compile.toList)
      },
      name = "projects events count limit"
    )
  // TODO use proper skunk encoder
  private implicit class StatusesOps(statuses: NonEmptyList[EventStatus]) {
    lazy val toSql: String = statuses.map(status => s"'$status'").mkString_(", ")
  }
}

object IOStatsFinder {

  import io.renku.eventlog.subscriptions.membersync

  def apply(
      transactor:          SessionResource[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[StatsFinder[IO]] = IO {
    new StatsFinderImpl(
      transactor,
      queriesExecTimes,
      categoryNames = Refined.unsafeApply(Set(membersync.categoryName))
    )
  }
}
