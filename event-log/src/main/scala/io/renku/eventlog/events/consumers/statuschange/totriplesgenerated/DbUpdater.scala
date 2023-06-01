/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.statuschange.totriplesgenerated

import cats.data.Kleisli
import cats.effect.Async
import cats.kernel.Monoid
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.implicits._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.api.events.StatusChangeEvent.ToTriplesGenerated
import io.renku.eventlog.events.consumers.statuschange
import io.renku.eventlog.events.consumers.statuschange.DBUpdater.{RollbackOp, UpdateOp}
import io.renku.eventlog.events.consumers.statuschange.{DBUpdateResults, DeliveryInfoRemover}
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{EventId, EventProcessingTime, EventStatus, ExecutionDate}
import io.renku.graph.model.projects
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private[statuschange] class DbUpdater[F[_]: Async: QueriesExecutionTimes](
    deliveryInfoRemover: DeliveryInfoRemover[F],
    now:                 () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with statuschange.DBUpdater[F, ToTriplesGenerated] {

  private lazy val partitionSize = 50

  import deliveryInfoRemover._

  override def onRollback(event: ToTriplesGenerated): RollbackOp[F] = deleteDelivery(event.eventId)

  override def updateDB(event: ToTriplesGenerated): UpdateOp[F] =
    deleteDelivery(event.eventId) >> updateStatus(event) >>= {
      case results if results.statusCounts.isEmpty => Kleisli.pure(results)
      case results => updateDependentData(event).map(_ combine results).widen[DBUpdateResults]
    }

  private def updateDependentData(event: ToTriplesGenerated) = for {
    _                                        <- updatePayload(event)
    _                                        <- updateProcessingTime(event)
    (idsAndStatuses, ancestorsUpdateResults) <- updateAncestorsStatus(event)
    _                                        <- cleanUp(idsAndStatuses, event)
  } yield ancestorsUpdateResults

  private def updateStatus(event: ToTriplesGenerated) = measureExecutionTime {
    SqlStatement(name = "to_triples_generated - status update")
      .command[ExecutionDate *: EventId *: projects.GitLabId *: EmptyTuple](
        sql"""UPDATE event evt
              SET status = '#${TriplesGenerated.value}', 
                execution_date = $executionDateEncoder, 
                message = NULL
              WHERE evt.event_id = $eventIdEncoder 
                AND evt.project_id = $projectIdEncoder 
                AND evt.status = '#${GeneratingTriples.value}'""".command
      )
      .arguments(ExecutionDate(now()) *: event.eventId.id *: event.eventId.projectId *: EmptyTuple)
      .build
      .flatMapResult {
        case Completion.Update(1) =>
          DBUpdateResults
            .ForProjects(event.project.path, Map(GeneratingTriples -> -1, TriplesGenerated -> 1))
            .pure[F]
        case Completion.Update(0) =>
          Monoid[DBUpdateResults.ForProjects].empty.pure[F]
        case completion =>
          new Exception(s"Could not update event ${event.eventId} to status $TriplesGenerated: $completion")
            .raiseError[F, DBUpdateResults.ForProjects]
      }
  }

  private def updatePayload(event: ToTriplesGenerated) = measureExecutionTime {
    SqlStatement(name = "to_triples_generated - payload upload")
      .command(
        sql"""INSERT INTO event_payload (event_id, project_id, payload)
              VALUES ($eventIdEncoder, $projectIdEncoder, $zippedPayloadEncoder)
              ON CONFLICT (event_id, project_id)
              DO UPDATE SET payload = EXCLUDED.payload;""".command
      )
      .arguments(event.eventId.id *: event.eventId.projectId *: event.payload *: EmptyTuple)
      .build
      .void
  }

  private def updateProcessingTime(event: ToTriplesGenerated) = measureExecutionTime {
    SqlStatement(name = "to_triples_generated - processing_time add")
      .command[EventId *: projects.GitLabId *: EventStatus *: EventProcessingTime *: EmptyTuple](
        sql"""INSERT INTO status_processing_time(event_id, project_id, status, processing_time)
              VALUES($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $eventProcessingTimeEncoder)
              ON CONFLICT (event_id, project_id, status)
              DO UPDATE SET processing_time = EXCLUDED.processing_time;
              """.command
      )
      .arguments(event.eventId.id *: event.eventId.projectId *: TriplesGenerated *: event.processingTime *: EmptyTuple)
      .build
      .void
  }

  private def updateAncestorsStatus(event: ToTriplesGenerated) = measureExecutionTime {
    SqlStatement(name = "to_triples_generated - ancestors update")
      .select[ExecutionDate *: projects.GitLabId *: projects.GitLabId *: EventId *: EventId *: EmptyTuple,
              (EventId, EventStatus)
      ](
        sql"""UPDATE event evt
              SET status = '#${TriplesGenerated.value}', 
                  execution_date = $executionDateEncoder, 
                  message = NULL
              FROM (
                SELECT event_id, project_id, status 
                FROM event
                WHERE project_id = $projectIdEncoder
                  AND #${`status IN`(Set(New, GeneratingTriples, GenerationRecoverableFailure, AwaitingDeletion))}
                  AND event_date < (
                    SELECT event_date
                    FROM event
                    WHERE project_id = $projectIdEncoder
                      AND event_id = $eventIdEncoder
                  )
                  AND event_id <> $eventIdEncoder
                FOR UPDATE
              ) old_evt
              WHERE evt.event_id = old_evt.event_id AND evt.project_id = old_evt.project_id 
              RETURNING evt.event_id, old_evt.status
           """.query(eventIdDecoder ~ eventStatusDecoder)
      )
      .arguments(
        ExecutionDate(now()) *:
          event.eventId.projectId *:
          event.eventId.projectId *:
          event.eventId.id *:
          event.eventId.id *:
          EmptyTuple
      )
      .build(_.toList)
      .mapResult { idsAndStatuses =>
        val decrementedStatuses = idsAndStatuses.groupMapReduce(_._2)(_ => -1)(_ + _)
        val incrementedStatuses =
          TriplesGenerated -> (idsAndStatuses.size - idsAndStatuses.count(_._2 == EventStatus.AwaitingDeletion))
        idsAndStatuses -> DBUpdateResults.ForProjects(
          event.project.path,
          decrementedStatuses + incrementedStatuses
        )
      }
  }

  private def cleanUp(idsAndStatuses: List[(EventId, EventStatus)],
                      event:          ToTriplesGenerated
  ): Kleisli[F, Session[F], Unit] = Kleisli { session =>
    idsAndStatuses
      .sliding(size = partitionSize, step = partitionSize)
      .map(executeRemovalQueries(event)(_).run(session))
      .toList
      .sequence
      .void
  }

  private def executeRemovalQueries(event: ToTriplesGenerated)(eventsWindow: List[(EventId, EventStatus)]) = for {
    _ <- removeAncestorsProcessingTimes(eventsWindow, event.eventId.projectId)
    _ <- removeAncestorsPayloads(eventsWindow, event.eventId.projectId)
    _ <- removeAwaitingDeletionEvents(eventsWindow, event)
  } yield ()

  private def removeAncestorsProcessingTimes(idsAndStatuses: List[(EventId, EventStatus)],
                                             projectId:      projects.GitLabId
  ) =
    idsAndStatuses.filterNot { case (_, status) => Set(New, GeneratingTriples, Skipped).contains(status) } match {
      case Nil => Kleisli.pure(())
      case eventIdsToRemove =>
        measureExecutionTime {
          SqlStatement(name = "to_triples_generated - processing_times removal")
            .command[projects.GitLabId](
              sql"""DELETE FROM status_processing_time
                    WHERE event_id IN (#${eventIdsToRemove.map { case (id, _) => s"'$id'" }.mkString(",")})
                      AND project_id = $projectIdEncoder""".command
            )
            .arguments(projectId)
            .build
            .void
        }
    }

  private def removeAncestorsPayloads(idsAndStatuses: List[(EventId, EventStatus)], projectId: projects.GitLabId) =
    idsAndStatuses.filterNot { case (_, status) =>
      Set(New, GeneratingTriples, Skipped).contains(status)
    } match {
      case Nil => Kleisli.pure(())
      case eventIdsToRemove =>
        measureExecutionTime {
          SqlStatement(name = "to_triples_generated - payloads removal")
            .command[projects.GitLabId](
              sql"""DELETE FROM event_payload
                    WHERE event_id IN (#${eventIdsToRemove.map { case (id, _) => s"'$id'" }.mkString(",")})
                      AND project_id = $projectIdEncoder""".command
            )
            .arguments(projectId)
            .build
            .void
        }
    }

  private def removeAwaitingDeletionEvents(eventsWindow: List[(EventId, EventStatus)], event: ToTriplesGenerated) =
    eventsWindow.collect { case (id, AwaitingDeletion) => id } match {
      case Nil => Kleisli.pure(DBUpdateResults.ForProjects(event.project.path, Map()))
      case eventIdsToRemove =>
        measureExecutionTime {
          SqlStatement(name = "to_triples_generated - awaiting_deletions removal")
            .command[projects.GitLabId](
              sql"""DELETE FROM event
                    WHERE event_id IN (#${eventIdsToRemove.map(id => s"'$id'").mkString(",")})
                      AND project_id = $projectIdEncoder""".command
            )
            .arguments(event.eventId.projectId)
            .build
            .void
        }
    }

  private def `status IN`(statuses: Set[EventStatus]) =
    s"status IN (${statuses.map(s => s"'$s'").toList.mkString(",")})"
}
