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

  import doobie.ConnectionIO
  import io.renku.eventlog.TypesSerializers._

  override def storeNewEvent(event: Event): IO[Result] =
    for {
      result <- insertIfNotDuplicate(event) transact transactor.get
      _ <- Applicative[IO].whenA(result == Created && event.status == New)(
             waitingEventsGauge.increment(event.project.path)
           )
    } yield result

  private def insertIfNotDuplicate(event: Event) =
    checkIfPersisted(event) flatMap {
      case true  => Free.pure[ConnectionOp, Result](Existed)
      case false => persist(event)
    }

  private def persist(event: Event): Free[ConnectionOp, Result] =
    for {
      updatedCommitEvent <- eventuallyAddToExistingBatch(event)
      _                  <- upsertProject(updatedCommitEvent)
      _                  <- insert(updatedCommitEvent)
    } yield Created

  private def eventuallyAddToExistingBatch(event: Event) =
    findBatchInQueue(event)
      .map(_.map(event.withBatchDate).getOrElse(event))

  private def checkIfPersisted(event: Event) = measureExecutionTime {
    SqlQuery(
      sql"""|SELECT event_id
            |FROM event
            |WHERE event_id = ${event.id} AND project_id = ${event.project.id}""".stripMargin
        .query[String]
        .option
        .map(_.isDefined),
      name = "new - check existence"
    )
  }

  // format: off
  private def findBatchInQueue(event: Event) = measureExecutionTime {
    SqlQuery({ fr"""
        SELECT batch_date
        FROM event
        WHERE project_id = ${event.project.id} AND """ ++ `status IN`(New, RecoverableFailure, Processing) ++ fr"""
        ORDER BY batch_date DESC
        LIMIT 1"""
      }.query[BatchDate].option,
      name = "new - find batch"
    )
  }
  // format: on

  private lazy val insert: Event => ConnectionIO[Unit] = {
    case NewEvent(id, project, date, batchDate, body) =>
      val currentTime = now()
      measureExecutionTime(
        SqlQuery(
          sql"""|INSERT INTO
                |event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body)
                |VALUES ($id, ${project.id}, ${New: EventStatus}, $currentTime, $currentTime, $date, $batchDate, $body)
                |""".stripMargin.update.run.map(_ => ()),
          name = "new - create (NEW)"
        )
      )
    case SkippedEvent(id, project, date, batchDate, body, message) =>
      val currentTime = now()
      measureExecutionTime(
        SqlQuery(
          sql"""|INSERT INTO
                |event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body, message)
                |VALUES ($id, ${project.id}, ${Skipped: EventStatus}, $currentTime, $currentTime, $date, $batchDate, $body, $message)
                |""".stripMargin.update.run.map(_ => ()),
          name = "new - create (SKIPPED)"
        )
      )
  }

  private def upsertProject(event: Event) = measureExecutionTime {
    SqlQuery(
      sql"""|INSERT INTO
            |project (project_id, project_path, latest_event_date)
            |VALUES (${event.project.id}, ${event.project.path}, ${event.date})
            |ON CONFLICT (project_id)
            |DO 
            |  UPDATE SET latest_event_date = EXCLUDED.latest_event_date, project_path = EXCLUDED.project_path 
            |  WHERE EXCLUDED.latest_event_date > project.latest_event_date
      """.stripMargin.update.run.map(_ => ()),
      name = "new - upsert project"
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
  ): IO[EventPersisterImpl] = IO {
    new EventPersisterImpl(transactor, waitingEventsGauge, queriesExecTimes)
  }
}
