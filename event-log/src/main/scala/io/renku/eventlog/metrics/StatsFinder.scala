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

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.effect.MonadCancelThrow
import cats.effect.kernel.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.db.implicits._
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.eventlog._
import io.renku.eventlog.subscriptions._
import io.renku.graph.model.events.{CategoryName, EventStatus}
import io.renku.graph.model.projects.Path
import io.renku.metrics.LabeledHistogram
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

class StatsFinderImpl[Interpretation[_]: MonadCancelThrow: Async](
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
    SqlStatement(name = "projects events count limit").select[Int, (Path, Long)](sql"""
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
  // format: on

  private implicit class StatusesOps(statuses: NonEmptyList[EventStatus]) {
    lazy val toSql: String = statuses.map(status => s"'$status'").mkString_(", ")
  }
}

object StatsFinder {

  def apply[F[_]: MonadCancelThrow: Async](
      sessionResource:  SessionResource[F, EventLogDB],
      queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[StatsFinder[F]] = MonadThrow[F].catchNonFatal {
    new StatsFinderImpl(sessionResource, queriesExecTimes)
  }
}
