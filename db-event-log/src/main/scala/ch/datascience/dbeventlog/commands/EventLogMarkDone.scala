/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import cats.MonadError
import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.db.TransactorProvider
import ch.datascience.dbeventlog.{EventStatus, IOTransactorProvider}
import ch.datascience.dbeventlog.EventStatus.{Processing, TriplesStore}
import ch.datascience.graph.model.events.CommitId
import doobie.implicits._

import scala.language.higherKinds

class EventLogMarkDone[Interpretation[_]](
    transactorProvider: TransactorProvider[Interpretation],
    now:                () => Instant = Instant.now
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  def markEventDone(eventId: CommitId): Interpretation[Unit] =
    for {
      transactor     <- transactorProvider.transactor
      updatedRecords <- update(eventId, newStatus = TriplesStore).transact(transactor)
      _              <- failIfNoRecordsUpdated(eventId)(updatedRecords)
    } yield ()

  private def update(eventId: CommitId, newStatus: EventStatus) =
    sql"""update event_log 
         |set status = $newStatus, execution_date = ${now()}
         |where event_id = $eventId and status = ${Processing: EventStatus}
         |""".stripMargin.update.run

  private def failIfNoRecordsUpdated(eventId: CommitId): Int => Interpretation[Unit] = {
    case 0 =>
      ME.raiseError(
        new RuntimeException(
          s"Event with id = $eventId couldn't be updated; Either no event or not with status $Processing"
        )
      )
    case _ => ME.unit
  }
}

class IOEventLogMarkDone(
    implicit contextShift: ContextShift[IO]
) extends EventLogMarkDone[IO](new IOTransactorProvider)
