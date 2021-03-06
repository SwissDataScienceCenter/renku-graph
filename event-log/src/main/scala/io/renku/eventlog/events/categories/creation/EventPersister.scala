/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.categories.creation

import cats.Applicative
import cats.data.{Kleisli, NonEmptyList}
import cats.effect.{BracketThrow, IO}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import eu.timepit.refined.auto._
import io.renku.eventlog.Event.{NewEvent, SkippedEvent}
import io.renku.eventlog.events.categories.creation.EventPersister.Result
import io.renku.eventlog.events.categories.creation.EventPersister.Result._
import io.renku.eventlog._
import skunk._
import skunk.implicits._

import java.time.Instant

trait EventPersister[Interpretation[_]] {
  def storeNewEvent(event: Event): Interpretation[Result]
}

class EventPersisterImpl[Interpretation[_]: BracketThrow](
    sessionResource:    SessionResource[Interpretation, EventLogDB],
    waitingEventsGauge: LabeledGauge[Interpretation, projects.Path],
    queriesExecTimes:   LabeledHistogram[Interpretation, SqlStatement.Name],
    now:                () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with EventPersister[Interpretation] {

  import io.renku.eventlog.TypeSerializers._

  override def storeNewEvent(event: Event): Interpretation[Result] = sessionResource.useWithTransactionK[Result] {
    Kleisli { case (transaction, session) =>
      for {
        sp <- transaction.savepoint
        result <- insertIfNotDuplicate(event)(session) recoverWith { case error =>
                    transaction.rollback(sp) >> error.raiseError[Interpretation, Result]
                  }
        _ <-
          Applicative[Interpretation].whenA(result == Created && event.status == New)(
            waitingEventsGauge.increment(event.project.path)
          )

      } yield result
    }
  }

  private def insertIfNotDuplicate(event: Event) =
    checkIfPersisted(event) >>= {
      case true  => Kleisli.pure(Existed: Result)
      case false => persist(event)
    }

  private def persist(event: Event): Kleisli[Interpretation, Session[Interpretation], Result] =
    for {
      updatedCommitEvent <- eventuallyAddToExistingBatch(event)
      _                  <- upsertProject(updatedCommitEvent)
      _                  <- insert(updatedCommitEvent)
    } yield Created

  private def eventuallyAddToExistingBatch(event: Event) =
    findBatchInQueue(event)
      .map(_.map(event.withBatchDate).getOrElse(event))

  private def checkIfPersisted(event: Event) = measureExecutionTime(
    SqlStatement(name = "new - check existence")
      .select[EventId ~ projects.Id, EventId](
        sql"""SELECT event_id
              FROM event
              WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
          """.query(eventIdDecoder)
      )
      .arguments(event.id ~ event.project.id)
      .build(_.option)
      .mapResult(_.isDefined)
  )

  private def findBatchInQueue(event: Event) = measureExecutionTime(
    SqlStatement(name = "new - find batch")
      .select[projects.Id, BatchDate](
        sql"""SELECT batch_date
              FROM event
              WHERE project_id = $projectIdEncoder AND #${`status IN`(New,
                                                                      GenerationRecoverableFailure,
                                                                      GeneratingTriples
        )}
              ORDER BY batch_date DESC
              LIMIT 1
          """.query(batchDateDecoder)
      )
      .arguments(event.project.id)
      .build(_.option)
  )

  private lazy val insert: Event => Kleisli[Interpretation, Session[Interpretation], Unit] = {
    case NewEvent(id, project, date, batchDate, body) =>
      val (createdDate, executionDate) = (CreatedDate.apply _ &&& ExecutionDate.apply _)(now())
      measureExecutionTime(
        SqlStatement(name = "new - create (NEW)")
          .command[
            EventId ~ projects.Id ~ EventStatus ~ CreatedDate ~ ExecutionDate ~ EventDate ~ BatchDate ~ EventBody
          ](
            sql"""INSERT INTO event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body)
                VALUES ($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $createdDateEncoder, $executionDateEncoder, $eventDateEncoder, $batchDateEncoder, $eventBodyEncoder)
              """.command
          )
          .arguments(id ~ project.id ~ New ~ createdDate ~ executionDate ~ date ~ batchDate ~ body)
          .build
          .void
      )
    case SkippedEvent(id, project, date, batchDate, body, message) =>
      val (createdDate, executionDate) = (CreatedDate.apply _ &&& ExecutionDate.apply _)(now())
      measureExecutionTime(
        SqlStatement(name = "new - create (SKIPPED)")
          .command[
            EventId ~ projects.Id ~ EventStatus ~ CreatedDate ~ ExecutionDate ~ EventDate ~ BatchDate ~ EventBody ~ EventMessage
          ](
            sql"""INSERT INTO
                  event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body, message)
                  VALUES ($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $createdDateEncoder, $executionDateEncoder, $eventDateEncoder, $batchDateEncoder, $eventBodyEncoder, $eventMessageEncoder)
              """.command
          )
          .arguments(id ~ project.id ~ Skipped ~ createdDate ~ executionDate ~ date ~ batchDate ~ body ~ message)
          .build
          .void
      )
  }

  private def upsertProject(event: Event) = measureExecutionTime(
    SqlStatement(name = "new - upsert project")
      .command[projects.Id ~ projects.Path ~ EventDate](
        sql"""
            INSERT INTO
            project (project_id, project_path, latest_event_date)
            VALUES ($projectIdEncoder, $projectPathEncoder, $eventDateEncoder)
            ON CONFLICT (project_id)
            DO 
              UPDATE SET latest_event_date = EXCLUDED.latest_event_date, project_path = EXCLUDED.project_path 
              WHERE EXCLUDED.latest_event_date > project.latest_event_date
          """.command
      )
      .arguments(event.project.id ~ event.project.path ~ event.date)
      .build
      .void
  )

  private def `status IN`(status: EventStatus, otherStatuses: EventStatus*) =
    s"status IN (${NonEmptyList.of(status, otherStatuses: _*).map(el => s"'$el'").toList.mkString(",")})"
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
      sessionResource:    SessionResource[IO, EventLogDB],
      waitingEventsGauge: LabeledGauge[IO, projects.Path],
      queriesExecTimes:   LabeledHistogram[IO, SqlStatement.Name]
  ): IO[EventPersisterImpl[IO]] = IO {
    new EventPersisterImpl[IO](sessionResource, waitingEventsGauge, queriesExecTimes)
  }
}
