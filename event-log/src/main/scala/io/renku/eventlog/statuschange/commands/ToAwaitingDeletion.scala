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

package io.renku.eventlog.statuschange.commands

import cats.Id
import cats.data.{Kleisli, NonEmptyList}
import cats.effect.BracketThrow
import cats.syntax.all._
import ch.datascience.db.{SessionResource, SqlStatement}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.eventlog.statuschange.ChangeStatusRequest.EventOnlyRequest
import io.renku.eventlog.statuschange.CommandFindingResult.{CommandFound, NotSupported}
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath
import io.renku.eventlog.statuschange.{ChangeStatusRequest, CommandFindingResult}
import io.renku.eventlog.{EventLogDB, ExecutionDate, TypeSerializers}
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant
import scala.language.postfixOps

final case class ToAwaitingDeletion[Interpretation[_]: BracketThrow](
    eventId:                            CompoundEventId,
    currentStatus:                      EventStatus,
    awaitingTriplesGenerationGauge:     LabeledGauge[Interpretation, projects.Path],
    underTriplesGenerationGauge:        LabeledGauge[Interpretation, projects.Path],
    awaitingTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    underTriplesTransformationGauge:    LabeledGauge[Interpretation, projects.Path],
    maybeProcessingTime:                Option[EventProcessingTime],
    now:                                () => Instant = () => Instant.now
) extends ChangeStatusCommand[Interpretation] {

  override lazy val status: EventStatus = AwaitingDeletion

  override def queries: NonEmptyList[SqlStatement[Interpretation, Int]] = NonEmptyList.of(
    SqlStatement[Interpretation](
      name = Refined.unsafeApply(s"$currentStatus->awaiting_generation")
    ).command[EventStatus ~ ExecutionDate ~ EventId ~ projects.Id](
      sql"""UPDATE event
                SET status = $eventStatusEncoder, execution_date = $executionDateEncoder
                WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
             """.command
    ).arguments(status ~ ExecutionDate(now()) ~ eventId.id ~ eventId.projectId)
      .build
      .flatMapResult {
        case Completion.Update(n) => n.pure[Interpretation]
        case completion =>
          new RuntimeException(
            s"$currentStatus->awaiting_generation query failed with completion status $completion"
          ).raiseError[Interpretation, Int]
      }
  )

  override def updateGauges(
      updateResult: UpdateResult
  ): Kleisli[Interpretation, Session[Interpretation], Unit] = updateResult match {
    case UpdateResult.Updated =>
      val gaugeToDecrease: projects.Path => Interpretation[Unit] = currentStatus match {
        case New                 => awaitingTriplesGenerationGauge decrement
        case GeneratingTriples   => underTriplesGenerationGauge decrement
        case TriplesGenerated    => awaitingTriplesTransformationGauge decrement
        case TransformingTriples => underTriplesTransformationGauge decrement
        case _                   => _ => ().pure[Interpretation]
      }
      for {
        path <- findProjectPath(eventId)
        _    <- Kleisli.liftF(gaugeToDecrease(path))
      } yield ()
    case _ => Kleisli.pure(())
  }
}

object ToAwaitingDeletion extends TypeSerializers {
  def factory[Interpretation[_]: BracketThrow](
      sessionResource:                    SessionResource[Interpretation, EventLogDB],
      awaitingTriplesGenerationGauge:     LabeledGauge[Interpretation, projects.Path],
      underTriplesGenerationGauge:        LabeledGauge[Interpretation, projects.Path],
      awaitingTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
      underTriplesTransformationGauge:    LabeledGauge[Interpretation, projects.Path]
  ): Kleisli[Interpretation, ChangeStatusRequest, CommandFindingResult] = Kleisli {
    case EventOnlyRequest(eventId, AwaitingDeletion, maybeProcessingTime, _) =>
      (findEventStatus[Interpretation](eventId, sessionResource) map { currentStatus =>
        CommandFound(
          ToAwaitingDeletion[Interpretation](
            eventId,
            currentStatus,
            awaitingTriplesGenerationGauge,
            underTriplesGenerationGauge,
            awaitingTriplesTransformationGauge,
            underTriplesTransformationGauge,
            maybeProcessingTime
          )
        )
      }).widen[CommandFindingResult]
    case _ => NotSupported.pure[Interpretation].widen[CommandFindingResult]
  }

  private def findEventStatus[Interpretation[_]: BracketThrow](
      eventId:         CompoundEventId,
      sessionResource: SessionResource[Interpretation, EventLogDB]
  ): Interpretation[EventStatus] = sessionResource.useK {
    SqlStatement(name = "to_awaiting_deletion find event status")
      .select[EventId ~ projects.Id, EventStatus](
        sql"""
            SELECT status
            FROM event
            WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder
            """.query(eventStatusDecoder)
      )
      .arguments(eventId.id ~ eventId.projectId)
      .build[Id](_.unique)
      .queryExecution
  }
}
