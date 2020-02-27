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

import java.time.Instant

import cats.effect.{Bracket, ContextShift, IO}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventStatus.New
import ch.datascience.dbeventlog.{EventLogDB, EventStatus}
import doobie.implicits._

import scala.language.higherKinds

class EventLogReScheduler[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    now:        () => Instant = () => Instant.now
)(implicit ME:  Bracket[Interpretation, Throwable]) {

  def scheduleEventsForProcessing(): Interpretation[Unit] =
    runUpdate()
      .transact(transactor.get)
      .map(_ => ())

  private def runUpdate() =
    sql"""|update event_log 
          |set status = ${New: EventStatus}, execution_date = ${now()}, message = NULL
          |""".stripMargin.update.run
}

class IOEventLogReScheduler(
    transactor:          DbTransactor[IO, EventLogDB]
)(implicit contextShift: ContextShift[IO])
    extends EventLogReScheduler[IO](transactor)
