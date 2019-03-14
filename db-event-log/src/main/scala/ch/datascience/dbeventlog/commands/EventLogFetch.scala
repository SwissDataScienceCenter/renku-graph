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
import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO}
import cats.free.Free
import cats.implicits._
import ch.datascience.db.TransactorProvider
import ch.datascience.dbeventlog.EventStatus.{New, TriplesStoreFailure}
import ch.datascience.dbeventlog.{EventBody, EventStatus, IOTransactorProvider}
import ch.datascience.graph.model.events.CommitId
import doobie.free.connection.ConnectionOp
import doobie.implicits._
import doobie.util.fragments._

import scala.language.higherKinds

class EventLogFetch[Interpretation[_]](
    transactorProvider: TransactorProvider[Interpretation],
    now:                () => Instant = Instant.now
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  import ModelReadsAndWrites._

  def findEventToProcess: Interpretation[Option[EventBody]] =
    for {
      transactor     <- transactorProvider.transactor
      maybeEventBody <- findEventAndUpdateForProcessing.transact(transactor)
    } yield maybeEventBody

  private def findEventAndUpdateForProcessing =
    for {
      maybeIdAndBody <- findOldestEvent
      maybeBody      <- markAsProcessing(maybeIdAndBody)
    } yield maybeBody

  // format: off
  private def findOldestEvent = {
    fr"""select event_id, event_body
         from event_log
         where """ ++ `status IN`(New, TriplesStoreFailure) ++ fr"""
           and execution_date < ${now()}
         order by execution_date asc
         limit 1"""
  }.query[(CommitId, EventBody)].option
  // format: on

  private def `status IN`(status: EventStatus, otherStatuses: EventStatus*) =
    in(fr"status", NonEmptyList.of(status, otherStatuses: _*))

  private lazy val markAsProcessing: Option[(CommitId, EventBody)] => Free[ConnectionOp, Option[EventBody]] = {
    case None => Free.pure[ConnectionOp, Option[EventBody]](None)
    case Some((eventId, eventBody)) =>
      sql"""update event_log 
           |set status = ${EventStatus.Processing: EventStatus}
           |where event_id = $eventId and status <> ${EventStatus.Processing: EventStatus}
           |""".stripMargin.update.run
        .map(toNoneIfEventAlreadyTaken(eventBody))
  }

  private def toNoneIfEventAlreadyTaken(eventBody: EventBody): Int => Option[EventBody] = {
    case 0 => None
    case 1 => Some(eventBody)
  }
}

class IOEventLogFetch(implicit contextShift: ContextShift[IO]) extends EventLogFetch[IO](new IOTransactorProvider)
