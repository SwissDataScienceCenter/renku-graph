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

package io.renku.eventlog.creation

import java.time.Instant

import EventPersister.Result
import Result._
import cats.Applicative
import cats.data.NonEmptyList
import cats.effect.{Bracket, IO}
import cats.free.Free
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import doobie.free.connection.ConnectionOp
import doobie.implicits._
import doobie.util.fragments.in
import eu.timepit.refined.auto._
import io.renku.eventlog.Event.{NewEvent, SkippedEvent}
import io.renku.eventlog.{Event, EventLogDB}

trait EventPersister[Interpretation[_]] {
  def storeNewEvent(event: Event): Interpretation[Result]
}

class EventPersisterImpl(
    transactor:         DbTransactor[IO, EventLogDB],
    waitingEventsGauge: LabeledGauge[IO, projects.Path],
    queriesExecTimes:   LabeledHistogram[IO, SqlQuery.Name],
    now:                () => Instant = () => Instant.now
)(implicit ME:          Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with EventPersister[IO] {

  import io.renku.eventlog.TypesSerializers._

  override def storeNewEvent(event: Event): IO[Result] =
    for {
      result <- insertIfNotDuplicate(event) transact transactor.get
      _ <- Applicative[IO].whenA(result == Created && event.status == New)(
             waitingEventsGauge.increment(event.project.path)
           )
    } yield result

  private def insertIfNotDuplicate(event: Event) =
    measureExecutionTime(checkIfInLog(event)) flatMap {
      case Some(_) => Free.pure[ConnectionOp, Result](Existed)
      case None    => addToLog(event)
    }

  private def addToLog(event: Event): Free[ConnectionOp, Result] =
    for {
      updatedCommitEvent <- eventuallyAddToExistingBatch(event)
      _                  <- measureExecutionTime(insert(updatedCommitEvent))
    } yield Created

  private def eventuallyAddToExistingBatch(event: Event) =
    measureExecutionTime(findBatchInQueue(event)).map(_.map(event.withBatchDate).getOrElse(event))

  private def checkIfInLog(event: Event) = SqlQuery(
    sql"""|select event_id
          |from event_log
          |where event_id = ${event.id} and project_id = ${event.project.id}""".stripMargin
      .query[String]
      .option,
    name = "new - check existence"
  )

  // format: off
  private def findBatchInQueue(event: Event) = SqlQuery({ fr"""
      select batch_date
      from event_log
      where project_id = ${event.project.id} and """ ++ `status IN`(New, RecoverableFailure, Processing) ++ fr"""
      order by batch_date desc
      limit 1"""
    }.query[BatchDate].option,
    name = "new - find batch"
  )
  // format: on

  private lazy val insert: Event => SqlQuery[Unit] = {
    case NewEvent(id, project, date, batchDate, body) =>
      val currentTime = now()
      SqlQuery(
        sql"""insert into
          event_log (event_id, project_id, project_path, status, created_date, execution_date, event_date, batch_date, event_body)
          values ($id, ${project.id}, ${project.path}, ${New: EventStatus}, $currentTime, $currentTime, $date, $batchDate, $body)
      """.update.run.map(_ => ()),
        name = "new - create (NEW)"
      )
    case SkippedEvent(id, project, date, batchDate, body, message) =>
      val currentTime = now()
      SqlQuery(
        sql"""insert into
          event_log (event_id, project_id, project_path, status, created_date, execution_date, event_date, batch_date, event_body, message)
          values ($id, ${project.id}, ${project.path}, ${Skipped: EventStatus}, $currentTime, $currentTime, $date, $batchDate, $body, $message)
      """.update.run.map(_ => ()),
        name = "new - create (SKIPPED)"
      )
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

object IOEventPersister {
  def apply(
      transactor:         DbTransactor[IO, EventLogDB],
      waitingEventsGauge: LabeledGauge[IO, projects.Path],
      queriesExecTimes:   LabeledHistogram[IO, SqlQuery.Name]
  ) = IO {
    new EventPersisterImpl(transactor, waitingEventsGauge, queriesExecTimes)
  }
}
