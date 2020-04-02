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
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.graph.model.events.CompoundEventId
import doobie.implicits._

import scala.language.higherKinds

class EventLogLatestEvents[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB]
)(implicit ME:  Bracket[Interpretation, Throwable]) {

  def findAllLatestEvents: Interpretation[List[CompoundEventId]] =
    sql"""
         |select log.event_id, log.project_id 
         |from event_log log, (select project_id, max(event_date) max_event_date from event_log group by project_id) aggregate
         |where log.project_id = aggregate.project_id and log.event_date = aggregate.max_event_date
         |""".stripMargin
      .query[CompoundEventId]
      .to[List]
      .transact(transactor.get)
}

class IOEventLogLatestEvents(
    transactor:          DbTransactor[IO, EventLogDB]
)(implicit contextShift: ContextShift[IO])
    extends EventLogLatestEvents[IO](transactor)
