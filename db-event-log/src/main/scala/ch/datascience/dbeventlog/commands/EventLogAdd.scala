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

import cats.data.NonEmptyList
import cats.effect.{Bracket, ContextShift, IO}
import cats.free.Free
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventStatus.{New, Processing, RecoverableFailure}
import ch.datascience.dbeventlog.{EventBody, EventLogDB, EventStatus}
import ch.datascience.graph.model.events._
import doobie.free.connection.ConnectionOp
import doobie.implicits._
import doobie.util.fragments.in

import scala.language.higherKinds

class EventLogAdd[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    now:        () => Instant = () => Instant.now
)(implicit ME:  Bracket[Interpretation, Throwable]) {

  def storeNewEvent(commitEvent: CommitEvent, eventBody: EventBody): Interpretation[Unit] =
    insertIfNotDuplicate(commitEvent, eventBody).transact(transactor.get)

  private def insertIfNotDuplicate(commitEvent: CommitEvent, eventBody: EventBody) =
    checkIfInLog(commitEvent) flatMap {
      case Some(_) => Free.pure[ConnectionOp, Unit](())
      case None    => addToLog(commitEvent, eventBody)
    }

  private def addToLog(commitEvent: CommitEvent, eventBody: EventBody) =
    for {
      updatedCommitEvent <- eventuallyAddToExistingBatch(commitEvent)
      _                  <- insert(updatedCommitEvent, eventBody)
    } yield ()

  private def eventuallyAddToExistingBatch(commitEvent: CommitEvent) = findBatchInQueue(commitEvent) map {
    case Some(batchDateUnderProcessing) => commitEvent.copy(batchDate = batchDateUnderProcessing)
    case _                              => commitEvent
  }

  private def checkIfInLog(commitEvent: CommitEvent) =
    sql"""|select event_id 
          |from event_log 
          |where event_id = ${commitEvent.id} and project_id = ${commitEvent.project.id}""".stripMargin
      .query[String]
      .option

  // format: off
  private def findBatchInQueue(commitEvent: CommitEvent) = { fr"""
    select batch_date 
    from event_log 
    where project_id = ${commitEvent.project.id} and """ ++ `status IN`(New, RecoverableFailure, Processing) ++ fr"""
    order by batch_date desc
    limit 1"""
    }.query[BatchDate].option
  // format: on

  private def insert(commitEvent: CommitEvent, eventBody: EventBody) = {
    import commitEvent._
    val currentTime = now()
    sql"""insert into 
          event_log (event_id, project_id, project_path, status, created_date, execution_date, event_date, batch_date, event_body) 
          values ($id, ${project.id}, ${project.path}, ${EventStatus.New: EventStatus}, $currentTime, $currentTime, $committedDate, $batchDate, $eventBody)
      """.update.run.map(_ => ())
  }

  private def `status IN`(status: EventStatus, otherStatuses: EventStatus*) =
    in(fr"status", NonEmptyList.of(status, otherStatuses: _*))
}

class IOEventLogAdd(
    transactor:          DbTransactor[IO, EventLogDB]
)(implicit contextShift: ContextShift[IO])
    extends EventLogAdd[IO](transactor)
