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
import cats.implicits._
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects.Path
import ch.datascience.metrics.LabeledHistogram
import doobie.implicits._
import doobie.util.fragments.in
import eu.timepit.refined.auto._
import io.renku.eventlog._

import scala.language.higherKinds

trait StatsFinder[Interpretation[_]] {
  def statuses: Interpretation[Map[EventStatus, Long]]
  def countEvents(statuses: Set[EventStatus]): Interpretation[Map[Path, Long]]
}

class StatsFinderImpl(
    transactor:       DbTransactor[IO, EventLogDB],
    queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
)(implicit ME:        Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with StatsFinder[IO]
    with TypesSerializers {

  override def statuses: IO[Map[EventStatus, Long]] =
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

  override def countEvents(statuses: Set[EventStatus]): IO[Map[Path, Long]] =
    NonEmptyList.fromList(statuses.toList) match {
      case None => Map.empty[Path, Long].pure[IO]
      case Some(statusesList) =>
        measureExecutionTime(countProjectsEvents(statusesList))
          .transact(transactor.get)
          .map(_.toMap)
    }

  // format: off
  private def countProjectsEvents(statuses: NonEmptyList[EventStatus]) = SqlQuery({
    fr"""
    |select project_path, count(event_id)
    |from event_log
    |where """ ++ `status IN`(statuses) ++ fr"""
    |group by project_path
    |"""
    }.stripMargin.query[(Path, Long)].to[List],
    name = "projects events count"
  )
  // format: on

  private def `status IN`(statuses: NonEmptyList[EventStatus]) = in(fr"status", statuses)
}

object IOStatsFinder {
  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[StatsFinder[IO]] = IO {
    new StatsFinderImpl(transactor, queriesExecTimes)
  }
}
