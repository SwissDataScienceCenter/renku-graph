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

import java.time.temporal.ChronoUnit.MINUTES
import java.time.{Duration, Instant}

import cats.data.NonEmptyList
import cats.effect.{Bracket, ContextShift, IO}
import cats.free.Free
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventStatus._
import ch.datascience.dbeventlog._
import ch.datascience.graph.model.events.CommitEventId
import doobie.free.connection.ConnectionOp
import doobie.implicits._
import doobie.util.fragments._

import scala.language.higherKinds

private object EventLogFetch {
  val MaxProcessingTime: Duration = Duration.of(10, MINUTES)
}

class EventLogFetch[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    now:        () => Instant = Instant.now
)(implicit ME:  Bracket[Interpretation, Throwable]) {

  import EventLogFetch._

  def findEventToProcess: Interpretation[Option[EventBody]] =
    findEventAndUpdateForProcessing.transact(transactor.get)

  private def findEventAndUpdateForProcessing =
    for {
      maybeIdAndProjectAndBody <- findOldestEvent
      maybeBody                <- markAsProcessing(maybeIdAndProjectAndBody)
    } yield maybeBody

  // format: off
  private def findOldestEvent = {
    fr"""select event_id, project_id, event_body
         from event_log
         where (""" ++ `status IN`(New, TriplesStoreFailure) ++ fr""" and execution_date < ${now()})
           or (status = ${Processing: EventStatus} and execution_date < ${now() minus MaxProcessingTime})
         order by execution_date asc
         limit 1"""
  }.query[EventIdAndBody].option
  // format: on

  private def `status IN`(status: EventStatus, otherStatuses: EventStatus*) =
    in(fr"status", NonEmptyList.of(status, otherStatuses: _*))

  private lazy val markAsProcessing: Option[EventIdAndBody] => Free[ConnectionOp, Option[EventBody]] = {
    case None => Free.pure[ConnectionOp, Option[EventBody]](None)
    case Some((commitEventId, eventBody)) =>
      sql"""update event_log 
           |set status = ${EventStatus.Processing: EventStatus}, execution_date = ${now()}
           |where (event_id = ${commitEventId.id} and project_id = ${commitEventId.projectId} and status <> ${Processing: EventStatus})
           |  or (event_id = ${commitEventId.id} and project_id = ${commitEventId.projectId} and status = ${Processing: EventStatus} and execution_date < ${now() minus MaxProcessingTime})
           |""".stripMargin.update.run
        .map(toNoneIfEventAlreadyTaken(eventBody))
  }

  private def toNoneIfEventAlreadyTaken(eventBody: EventBody): Int => Option[EventBody] = {
    case 0 => None
    case 1 => Some(eventBody)
  }

  private type EventIdAndBody = (CommitEventId, EventBody)
}

class IOEventLogFetch(
    transactor:          DbTransactor[IO, EventLogDB]
)(implicit contextShift: ContextShift[IO])
    extends EventLogFetch[IO](transactor)
