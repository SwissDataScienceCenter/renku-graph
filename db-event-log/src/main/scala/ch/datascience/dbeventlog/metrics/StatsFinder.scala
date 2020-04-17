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

package ch.datascience.dbeventlog.metrics

import cats.data.NonEmptyList
import cats.effect.{Bracket, ContextShift, IO}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog._
import ch.datascience.graph.model.projects.Path
import doobie.implicits._
import doobie.util.fragments.in

import scala.language.higherKinds

trait StatsFinder[Interpretation[_]] {
  def statuses: Interpretation[Map[EventStatus, Long]]
  def countEvents(statuses: Set[EventStatus]): Interpretation[Map[Path, Long]]
}

class StatsFinderImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends StatsFinder[Interpretation]
    with TypesSerializers {

  override def statuses: Interpretation[Map[EventStatus, Long]] =
    sql"""select status, count(event_id) from event_log group by status;""".stripMargin
      .query[(EventStatus, Long)]
      .to[List]
      .transact(transactor.get)
      .map(_.toMap)
      .map(addMissingStatues)

  private def addMissingStatues(stats: Map[EventStatus, Long]): Map[EventStatus, Long] =
    EventStatus.all.map(status => status -> stats.getOrElse(status, 0L)).toMap

  // format: off
  override def countEvents(statuses: Set[EventStatus]): Interpretation[Map[Path, Long]] =
    NonEmptyList.fromList(statuses.toList) match {
      case None =>               Map.empty[Path, Long].pure[Interpretation]
      case Some(statusesList) => (fr"""
    |select project_path, count(event_id)
    |from event_log
    |where """ ++ `status IN`(statusesList) ++ fr"""
    |group by project_path
    |""").stripMargin
          .query[(Path, Long)]
          .to[List]
          .transact(transactor.get)
          .map(_.toMap)
          }
  // format: on

  private def `status IN`(statuses: NonEmptyList[EventStatus]) = in(fr"status", statuses)
}

object IOStatsFinder {
  def apply(
      transactor:          DbTransactor[IO, EventLogDB]
  )(implicit contextShift: ContextShift[IO]): IO[StatsFinder[IO]] = IO {
    new StatsFinderImpl[IO](transactor)
  }
}
