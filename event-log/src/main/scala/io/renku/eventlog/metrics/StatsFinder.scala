/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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
import ch.datascience.graph.model.events.EventStatus
import ch.datascience.graph.model.projects.Path
import ch.datascience.metrics.LabeledHistogram
import doobie.implicits._
import doobie.util.fragment.Fragment
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog._

trait StatsFinder[Interpretation[_]] {
  def statuses(): Interpretation[Map[EventStatus, Long]]
  def countEvents(statuses:   Set[EventStatus],
                  maybeLimit: Option[Int Refined Positive] = None
  ): Interpretation[Map[Path, Long]]
}

class StatsFinderImpl(
    transactor:       DbTransactor[IO, EventLogDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
)(implicit ME:        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with StatsFinder[IO]
    with TypesSerializers {

  override def statuses(): IO[Map[EventStatus, Long]] =
    measureExecutionTime(findStatuses)
      .transact(transactor.get)
      .map(_.toMap)
      .map(addMissingStatues)

  private lazy val findStatuses = SqlQuery(
    sql"""select status, count(event_id) from event_log group by status;""".stripMargin
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
        s"""|select
            |  project_path,
            |  (select count(event_id) from event_log el_int where el_int.project_id = prj.project_id and status IN (${statuses.toSql})) as count
            |from project prj
            |where exists (
            |        select project_id
            |        from event_log el
            |        where el.project_id = prj.project_id and status IN (${statuses.toSql})
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
        s"""|select
            |  project_path,
            |  (select count(event_id) from event_log el_int where el_int.project_id = prj.project_id and status IN (${statuses.toSql})) as count
            |from (select project_id, project_path, latest_event_date
            |      from project
            |      order by latest_event_date desc) prj
            |where exists (
            |        select project_id
            |        from event_log el
            |        where el.project_id = prj.project_id and status IN (${statuses.toSql})
            |      )
            |limit $limit;
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
