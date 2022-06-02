/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import cats.{Applicative, MonadThrow}
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog._
import io.renku.eventlog.events.categories.creation.Event.{NewEvent, SkippedEvent}
import io.renku.eventlog.events.categories.creation.EventPersister.Result
import io.renku.eventlog.events.categories.creation.EventPersister.Result._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram}
import skunk._
import skunk.implicits._

import java.time.Instant

private trait EventPersister[F[_]] {
  def storeNewEvent(event: Event): F[Result]
}

private class EventPersisterImpl[F[_]: MonadCancelThrow: SessionResource](
    waitingEventsGauge: LabeledGauge[F, projects.Path],
    queriesExecTimes:   LabeledHistogram[F],
    now:                () => Instant = () => Instant.now
) extends DbClient(Some(queriesExecTimes))
    with EventPersister[F] {

  private val applicative = Applicative[F]
  import applicative._
  import io.renku.eventlog.TypeSerializers._

  override def storeNewEvent(event: Event): F[Result] = SessionResource[F].useWithTransactionK[Result] {
    Kleisli { case (transaction, session) =>
      for {
        sp <- transaction.savepoint
        result <- insertIfNotDuplicate(event)(session)
                    .flatTap(_ => transaction.commit)
                    .recoverWith { case error => transaction.rollback(sp) >> error.raiseError[F, Result] }
        _ <- whenA(aNewEventIsCreated(result))(waitingEventsGauge.increment(event.project.path))
      } yield result
    }
  }

  private lazy val aNewEventIsCreated: Result => Boolean = {
    case Created(event) if event.status == New => true
    case _                                     => false
  }

  private def insertIfNotDuplicate(event: Event) = checkIfPersisted(event) >>= {
    case true  => Kleisli.pure(Existed: Result)
    case false => persist(event)
  }

  private def persist(event: Event): Kleisli[F, Session[F], Result] = for {
    updatedCommitEvent <- eventuallyAddToExistingBatch(event) >>= eventuallyUpdateStatus
    _                  <- upsertProject(updatedCommitEvent)
    _                  <- insert(updatedCommitEvent)
  } yield Created(updatedCommitEvent)

  private def eventuallyAddToExistingBatch(event: Event) =
    findBatchInQueue(event)
      .map(_.map(event.withBatchDate).getOrElse(event))

  private lazy val eventuallyUpdateStatus: Event => Kleisli[F, Session[F], Event] = {
    case event: NewEvent =>
      findNewerEventStatus(event).map {
        case Some(newerStatus) => event.copy(status = newerStatus)
        case None              => event
      }
    case event => Kleisli.pure(event)
  }

  private def findNewerEventStatus(event: Event) = measureExecutionTime(
    SqlStatement(name = "new - find newer event status")
      .select[projects.Id ~ EventStatus ~ EventDate, EventStatus](
        sql"""SELECT status
              FROM event
              WHERE project_id = $projectIdEncoder AND status = $eventStatusEncoder
                    AND event_date >= $eventDateEncoder
              LIMIT 1
          """.query(eventStatusDecoder)
      )
      .arguments(event.project.id ~ TriplesStore ~ event.date)
      .build(_.option)
  )

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
              WHERE project_id = $projectIdEncoder 
                AND #${`status IN`(New, GenerationRecoverableFailure, GeneratingTriples)}
              ORDER BY batch_date DESC
              LIMIT 1
          """.query(batchDateDecoder)
      )
      .arguments(event.project.id)
      .build(_.option)
  )

  private lazy val insert: Event => Kleisli[F, Session[F], Unit] = {
    case NewEvent(id, project, date, batchDate, body, status) =>
      val (createdDate, executionDate) = (CreatedDate.apply _ &&& ExecutionDate.apply)(now())
      measureExecutionTime(
        SqlStatement(name = "new - create (NEW)")
          .command[
            EventId ~ projects.Id ~ EventStatus ~ CreatedDate ~ ExecutionDate ~ EventDate ~ BatchDate ~ EventBody
          ](
            sql"""INSERT INTO event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body)
                  VALUES ($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $createdDateEncoder, $executionDateEncoder, $eventDateEncoder, $batchDateEncoder, $eventBodyEncoder)
                  ON CONFLICT (event_id, project_id)
                  DO UPDATE SET event_date = EXCLUDED.event_date
              """.command
          )
          .arguments(id ~ project.id ~ status ~ createdDate ~ executionDate ~ date ~ batchDate ~ body)
          .build
          .void
      )
    case SkippedEvent(id, project, date, batchDate, body, message) =>
      val (createdDate, executionDate) = (CreatedDate.apply _ &&& ExecutionDate.apply)(now())
      measureExecutionTime(
        SqlStatement(name = "new - create (SKIPPED)")
          .command[
            EventId ~ projects.Id ~ EventStatus ~ CreatedDate ~ ExecutionDate ~ EventDate ~ BatchDate ~ EventBody ~ EventMessage
          ](
            sql"""INSERT INTO event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body, message)
                  VALUES ($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $createdDateEncoder, $executionDateEncoder, $eventDateEncoder, $batchDateEncoder, $eventBodyEncoder, $eventMessageEncoder)
                  ON CONFLICT (event_id, project_id)
                  DO UPDATE SET event_date = EXCLUDED.event_date
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
            INSERT INTO project (project_id, project_path, latest_event_date)
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

private object EventPersister {
  def apply[F[_]: MonadCancelThrow: SessionResource](
      waitingEventsGauge: LabeledGauge[F, projects.Path],
      queriesExecTimes:   LabeledHistogram[F]
  ): F[EventPersisterImpl[F]] = MonadThrow[F].catchNonFatal {
    new EventPersisterImpl[F](waitingEventsGauge, queriesExecTimes)
  }

  sealed trait Result extends Product with Serializable
  object Result {
    case class Created(event: Event) extends Result
    case object Existed              extends Result
  }
}
