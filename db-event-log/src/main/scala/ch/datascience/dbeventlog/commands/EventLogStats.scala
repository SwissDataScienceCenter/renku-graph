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

package ch.datascience.dbeventlog.commands

import cats.effect.{Bracket, ContextShift, IO}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventStatus.New
import ch.datascience.dbeventlog._
import ch.datascience.graph.model.projects.ProjectPath
import doobie.implicits._

import scala.language.higherKinds

trait EventLogStats[Interpretation[_]] {
  def statuses:      Interpretation[Map[EventStatus, Long]]
  def waitingEvents: Interpretation[Map[ProjectPath, Long]]
}

class EventLogStatsImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends EventLogStats[Interpretation] {

  override def statuses: Interpretation[Map[EventStatus, Long]] =
    sql"""select status, count(event_id) from event_log group by status;""".stripMargin
      .query[(EventStatus, Long)]
      .to[List]
      .transact(transactor.get)
      .map(_.toMap)
      .map(addMissingStatues)

  private def addMissingStatues(stats: Map[EventStatus, Long]): Map[EventStatus, Long] =
    EventStatus.all.map(status => status -> stats.getOrElse(status, 0L)).toMap

  override def waitingEvents: Interpretation[Map[ProjectPath, Long]] =
    sql"""|select project_path, sum(events)
          |from (
          |  select project_path, count(event_id) as events 
          |  from event_log 
          |  where status = ${New: EventStatus} 
          |  group by project_path
          |  union
          |  select distinct project_path, 0 as events
          |  from event_log
          |) counts
          |group by project_path; 
          |""".stripMargin
      .query[(ProjectPath, Long)]
      .to[List]
      .transact(transactor.get)
      .map(_.toMap)
}

class IOEventLogStats(
    transactor:          DbTransactor[IO, EventLogDB]
)(implicit contextShift: ContextShift[IO])
    extends EventLogStatsImpl[IO](transactor)
