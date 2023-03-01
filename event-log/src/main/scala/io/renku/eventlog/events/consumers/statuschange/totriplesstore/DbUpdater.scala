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

package io.renku.eventlog.events.consumers.statuschange.totriplesstore

import cats.data.Kleisli
import cats.effect.Async
import cats.kernel.Monoid
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.db.implicits._
import io.renku.eventlog.TypeSerializers._
import io.renku.eventlog.events.consumers.statuschange
import io.renku.eventlog.events.consumers.statuschange._
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events.{EventId, EventProcessingTime, EventStatus, ExecutionDate}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.projects
import skunk.{Session, ~}
import skunk.SqlState.DeadlockDetected
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

private[statuschange] class DbUpdater[F[_]: Async: QueriesExecutionTimes](
    deliveryInfoRemover: DeliveryInfoRemover[F],
    now:                 () => Instant = () => Instant.now
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with statuschange.DBUpdater[F, ToTriplesStore] {

  import deliveryInfoRemover._

  override def onRollback(event: ToTriplesStore) = deleteDelivery(event.eventId)

  override def updateDB(event: ToTriplesStore): UpdateResult[F] = for {
    _                      <- deleteDelivery(event.eventId)
    updateResults          <- updateEvent(event)
    ancestorsUpdateResults <- updateAncestors(event, updateResults) recoverWith retryOnDeadlock(event, updateResults)
  } yield updateResults combine ancestorsUpdateResults

  private def updateEvent(event: ToTriplesStore) = for {
    updateResults <- updateStatus(event)
    _             <- updateProcessingTime(event)
  } yield updateResults

  private def updateStatus(event: ToTriplesStore) = measureExecutionTime {
    SqlStatement(name = "to_triples_store - status update")
      .command[ExecutionDate ~ EventId ~ projects.GitLabId](
        sql"""UPDATE event evt
              SET status = '#${EventStatus.TriplesStore.value}',
                execution_date = $executionDateEncoder,
                message = NULL
              WHERE evt.event_id = $eventIdEncoder 
                AND evt.project_id = $projectIdEncoder 
                AND evt.status = '#${EventStatus.TransformingTriples.value}'""".command
      )
      .arguments(ExecutionDate(now()) ~ event.eventId.id ~ event.eventId.projectId)
      .build
      .flatMapResult {
        case Completion.Update(1) =>
          DBUpdateResults
            .ForProjects(event.projectPath, Map(TransformingTriples -> -1, TriplesStore -> 1))
            .pure[F]
        case Completion.Update(0) => Monoid[DBUpdateResults.ForProjects].empty.pure[F]
        case completion =>
          new Exception(s"Could not update event ${event.eventId} to status ${EventStatus.TriplesStore}: $completion")
            .raiseError[F, DBUpdateResults.ForProjects]
      }
  }

  private def updateProcessingTime(event: ToTriplesStore) = measureExecutionTime {
    SqlStatement(name = "to_triples_store - processing_time add")
      .command[EventId ~ projects.GitLabId ~ EventStatus ~ EventProcessingTime](
        sql"""INSERT INTO status_processing_time(event_id, project_id, status, processing_time)
              VALUES($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $eventProcessingTimeEncoder)
              ON CONFLICT (event_id, project_id, status)
              DO UPDATE SET processing_time = EXCLUDED.processing_time;
              """.command
      )
      .arguments(event.eventId.id ~ event.eventId.projectId ~ EventStatus.TriplesStore ~ event.processingTime)
      .build
      .void
  }

  private def updateAncestors(event: ToTriplesStore, updateResults: DBUpdateResults.ForProjects) =
    updateResults match {
      case results @ DBUpdateResults.ForProjects.empty => Kleisli.pure(results)
      case _ =>
        measureExecutionTime {
          val statusesToUpdate = Set(New,
                                     GeneratingTriples,
                                     GenerationRecoverableFailure,
                                     TriplesGenerated,
                                     TransformingTriples,
                                     TransformationRecoverableFailure
          )
          SqlStatement(name = "to_triples_store - ancestors update")
            .select[ExecutionDate ~ projects.GitLabId ~ projects.GitLabId ~ EventId ~ EventId, EventStatus](
              sql"""UPDATE event evt
                SET status = '#${EventStatus.TriplesStore.value}',
                    execution_date = $executionDateEncoder,
                    message = NULL
                FROM (
                  SELECT event_id, project_id, status 
                  FROM event
                  WHERE project_id = $projectIdEncoder
                    AND #${`status IN`(statusesToUpdate)}
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
                RETURNING old_evt.status
         """.query(eventStatusDecoder)
            )
            .arguments(
              ExecutionDate(
                now()
              ) ~ event.eventId.projectId ~ event.eventId.projectId ~ event.eventId.id ~ event.eventId.id
            )
            .build(_.toList)
            .mapResult { oldStatuses =>
              val decrementedStatuses = oldStatuses.groupBy(identity).view.mapValues(-_.size).toMap
              val incrementedStatuses = TriplesStore -> oldStatuses.size
              DBUpdateResults.ForProjects(event.projectPath, decrementedStatuses + incrementedStatuses)
            }
        }
    }

  private def retryOnDeadlock(
      event:         ToTriplesStore,
      updateResults: DBUpdateResults.ForProjects
  ): PartialFunction[Throwable, Kleisli[F, Session[F], DBUpdateResults.ForProjects]] = { case DeadlockDetected(_) =>
    updateAncestors(event, updateResults)
  }

  private def `status IN`(statuses: Set[EventStatus]) =
    s"status IN (${statuses.map(s => s"'$s'").toList.mkString(",")})"
}
