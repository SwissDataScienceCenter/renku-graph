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

import cats.effect.{Bracket, ContextShift, IO}
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventStatus._
import ch.datascience.dbeventlog._
import ch.datascience.dbeventlog.commands.ExecutionDateCalculator.StatusBasedCalculator
import ch.datascience.graph.model.events._
import doobie.implicits._

import scala.language.higherKinds

class EventLogMarkFailed[Interpretation[_]](
    transactor:              DbTransactor[Interpretation, EventLogDB],
    executionDateCalculator: ExecutionDateCalculator = new ExecutionDateCalculator()
)(implicit ME:               Bracket[Interpretation, Throwable]) {

  import executionDateCalculator._

  def markEventFailed(commitEventId: CommitEventId,
                      status:        FailureStatus,
                      maybeMessage:  Option[EventMessage]): Interpretation[Unit] =
    findEventAndUpdate(commitEventId, status, maybeMessage).transact(transactor.get)

  private def findEventAndUpdate(commitEventId: CommitEventId,
                                 status:        FailureStatus,
                                 maybeMessage:  Option[EventMessage]) =
    for {
      createdAndExecution <- findEventDates(commitEventId)
      executionDate = findExecutionDate(createdAndExecution, status)
      _ <- updateEvent(commitEventId, status, executionDate, maybeMessage)
    } yield ()

  private def findEventDates(commitEventId: CommitEventId): doobie.ConnectionIO[(CreatedDate, ExecutionDate)] =
    sql"""
         |select created_date, execution_date
         |from event_log
         |where event_id = ${commitEventId.id} and project_id = ${commitEventId.projectId}""".stripMargin
      .query[(CreatedDate, ExecutionDate)]
      .unique

  private def findExecutionDate(createdAndExecution: (CreatedDate, ExecutionDate), status: FailureStatus) = {
    val (createDate, executionDate) = createdAndExecution
    status match {
      case NonRecoverableFailure => newExecutionDate[NonRecoverableFailure](createDate, executionDate)
      case TriplesStoreFailure   => newExecutionDate[TriplesStoreFailure](createDate, executionDate)
    }
  }

  private def updateEvent(commitEventId: CommitEventId,
                          newStatus:     FailureStatus,
                          executionDate: ExecutionDate,
                          maybeMessage:  Option[EventMessage]) =
    sql"""update event_log 
         |set status = ${newStatus: EventStatus}, execution_date = $executionDate, message = $maybeMessage
         |where event_id = ${commitEventId.id} and project_id = ${commitEventId.projectId} and status = ${Processing: EventStatus}
         |""".stripMargin.update.run
}

class ExecutionDateCalculator(now: () => Instant = () => Instant.now) {

  def newExecutionDate[T <: FailureStatus](
      createdDate:      CreatedDate,
      executionDate:    ExecutionDate
  )(implicit calculate: StatusBasedCalculator[T]): ExecutionDate =
    calculate(createdDate, executionDate, now())

}

object ExecutionDateCalculator {

  import Math._
  import java.time._
  import java.time.temporal.ChronoUnit._

  trait StatusBasedCalculator[T <: FailureStatus] extends ((CreatedDate, ExecutionDate, Instant) => ExecutionDate)

  implicit val nonRecoverableFailureCalculator: StatusBasedCalculator[NonRecoverableFailure] =
    (_, _, now) => ExecutionDate(now)

  implicit val triplesStoreFailureCalculator: StatusBasedCalculator[TriplesStoreFailure] =
    (createdDate, executionDate, now) => {
      val datesDifference = Duration.between(createdDate.value, executionDate.value).getSeconds

      if (!((executionDate.value plus (datesDifference, SECONDS)) isAfter now)) ExecutionDate(now plus (10, SECONDS))
      else ExecutionDate(executionDate.value plus (datesDifference * log(datesDifference).toLong, SECONDS))
    }
}

class IOEventLogMarkFailed(
    transactor:          DbTransactor[IO, EventLogDB]
)(implicit contextShift: ContextShift[IO])
    extends EventLogMarkFailed[IO](transactor)
