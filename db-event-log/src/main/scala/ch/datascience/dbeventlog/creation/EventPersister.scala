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

package ch.datascience.dbeventlog.creation

import java.time.Instant

import cats.data.NonEmptyList
import cats.effect.{Bracket, ContextShift, IO}
import cats.free.Free
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventStatus.{New, Processing, RecoverableFailure}
import ch.datascience.dbeventlog.TypesSerializers._
import ch.datascience.dbeventlog.{Event, EventLogDB, EventStatus}
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import doobie.free.connection.ConnectionOp
import doobie.implicits._
import doobie.util.fragments.in

import scala.language.higherKinds

class EventPersister[Interpretation[_]](
    transactor:         DbTransactor[Interpretation, EventLogDB],
    waitingEventsGauge: LabeledGauge[Interpretation, projects.Path],
    now:                () => Instant = () => Instant.now
)(implicit ME:          Bracket[Interpretation, Throwable]) {

  import EventPersister.Result
  import Result._

  def storeNewEvent(event: Event): Interpretation[Result] =
    for {
      result <- insertIfNotDuplicate(event).transact(transactor.get)
      _      <- if (result == Created) waitingEventsGauge.increment(event.project.path) else ME.unit
    } yield result

  private def insertIfNotDuplicate(event: Event) =
    checkIfInLog(event) flatMap {
      case Some(_) => Free.pure[ConnectionOp, Result](Existed)
      case None    => addToLog(event)
    }

  private def addToLog(event: Event): Free[ConnectionOp, Result] =
    for {
      updatedCommitEvent <- eventuallyAddToExistingBatch(event)
      _                  <- insert(updatedCommitEvent)
    } yield Created

  private def eventuallyAddToExistingBatch(event: Event) = findBatchInQueue(event) map {
    case Some(batchDateUnderProcessing) => event.copy(batchDate = batchDateUnderProcessing)
    case _                              => event
  }

  private def checkIfInLog(event: Event) =
    sql"""|select event_id
          |from event_log
          |where event_id = ${event.id} and project_id = ${event.project.id}""".stripMargin
      .query[String]
      .option

  // format: off
  private def findBatchInQueue(event: Event) = { fr"""
    select batch_date
    from event_log
    where project_id = ${event.project.id} and """ ++ `status IN`(New, RecoverableFailure, Processing) ++ fr"""
    order by batch_date desc
    limit 1"""
    }.query[BatchDate].option
  // format: on

  private def insert(event: Event) = {
    import event._
    val currentTime = now()
    sql"""insert into
          event_log (event_id, project_id, project_path, status, created_date, execution_date, event_date, batch_date, event_body)
          values ($id, ${project.id}, ${project.path}, ${EventStatus.New: EventStatus}, $currentTime, $currentTime, $date, $batchDate, $body)
      """.update.run.map(_ => ())
  }

  private def `status IN`(status: EventStatus, otherStatuses: EventStatus*) =
    in(fr"status", NonEmptyList.of(status, otherStatuses: _*))
}

object EventPersister {

  sealed trait Result extends Product with Serializable

  object Result {
    case object Created extends Result
    case object Existed extends Result
  }
}

class IOEventPersister(
    transactor:          DbTransactor[IO, EventLogDB],
    waitingEventsGauge:  LabeledGauge[IO, projects.Path]
)(implicit contextShift: ContextShift[IO])
    extends EventPersister[IO](transactor, waitingEventsGauge)
