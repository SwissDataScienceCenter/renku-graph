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
import ch.datascience.dbeventlog.EventStatus.{Processing, TriplesStore}
import ch.datascience.dbeventlog.{EventLogDB, EventStatus}
import ch.datascience.graph.model.events.CompoundEventId
import doobie.implicits._

import scala.language.higherKinds

class EventLogMarkDone[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    now:        () => Instant = () => Instant.now
)(implicit ME:  Bracket[Interpretation, Throwable]) {

  def markEventDone(commitEventId: CompoundEventId): Interpretation[Unit] =
    for {
      updatedRecords <- update(commitEventId, newStatus = TriplesStore).transact(transactor.get)
      _              <- failIfMoreThanOneRecordUpdated(commitEventId)(updatedRecords)
    } yield ()

  private def update(commitEventId: CompoundEventId, newStatus: EventStatus) =
    sql"""update event_log 
         |set status = $newStatus, execution_date = ${now()}
         |where event_id = ${commitEventId.id} and project_id = ${commitEventId.projectId} and status = ${Processing: EventStatus}
         |""".stripMargin.update.run

  private def failIfMoreThanOneRecordUpdated(commitEventId: CompoundEventId): Int => Interpretation[Unit] = {
    case 0 => ME.unit
    case 1 => ME.unit
    case _ =>
      ME.raiseError(
        new RuntimeException(
          s"An attempt to set status $Processing on multiple rows with eventId = ${commitEventId.id} and projectId = ${commitEventId.projectId}"
        )
      )
  }
}

class IOEventLogMarkDone(
    transactor:          DbTransactor[IO, EventLogDB]
)(implicit contextShift: ContextShift[IO])
    extends EventLogMarkDone[IO](transactor)
