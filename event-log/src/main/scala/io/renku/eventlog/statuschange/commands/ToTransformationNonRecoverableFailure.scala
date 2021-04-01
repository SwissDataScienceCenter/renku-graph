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

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.{Async, Bracket}
import cats.syntax.all._
import ch.datascience.db.SqlQuery
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import eu.timepit.refined.auto._
import io.renku.eventlog.statuschange.commands.CommandFindingResult.CommandFound
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath
import io.renku.eventlog.{EventMessage, ExecutionDate}
import org.http4s.{MediaType, Request}
import skunk.{Command, _}
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

final case class ToTransformationNonRecoverableFailure[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    eventId:                         CompoundEventId,
    message:                         EventMessage,
    underTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    maybeProcessingTime:             Option[EventProcessingTime],
    now:                             () => Instant = () => Instant.now
) extends ChangeStatusCommand[Interpretation] {

  override lazy val status: EventStatus = TransformationNonRecoverableFailure

  override def queries: NonEmptyList[SqlQuery[Interpretation, Int]] = NonEmptyList.of(
    SqlQuery(
      Kleisli { session =>
        val query: Command[EventStatus ~ ExecutionDate ~ EventMessage ~ EventId ~ projects.Id ~ EventStatus] =
          sql"""UPDATE event
                SET status = $eventStatusPut, execution_date = $executionDatePut, message = $eventMessagePut
                WHERE event_id = $eventIdPut AND project_id = $projectIdPut AND status = $eventStatusPut
             """.command
        session
          .prepare(query)
          .use(
            _.execute(status ~ ExecutionDate(now()) ~ message ~ eventId.id ~ eventId.projectId ~ TransformingTriples)
          )
          .map {
            case Completion.Update(n) => n
            case completion =>
              throw new RuntimeException(
                s"transforming_triples->transformation_non_recoverable_fail time query failed with completion status $completion"
              )
          }
      },
      name = "transforming_triples->transformation_non_recoverable_fail"
    )
  )

  override def updateGauges(
      updateResult:   UpdateResult
  )(implicit session: Session[Interpretation]): Interpretation[Unit] = updateResult match {
    case UpdateResult.Updated => findProjectPath(eventId) flatMap underTriplesTransformationGauge.decrement
    case _                    => ().pure[Interpretation]
  }
}

object ToTransformationNonRecoverableFailure {
  def factory[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      underTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path]
  ): Kleisli[Interpretation, (CompoundEventId, Request[Interpretation]), CommandFindingResult] =
    Kleisli { case (eventId, request) =>
      when(request, has = MediaType.application.json) {
        {
          for {
            _                   <- request.validate(status = TransformationNonRecoverableFailure)
            maybeProcessingTime <- request.getProcessingTime
            message             <- request.message
          } yield CommandFound(
            ToTransformationNonRecoverableFailure[Interpretation](
              eventId,
              message,
              underTriplesTransformationGauge,
              maybeProcessingTime
            )
          )
        }.merge
      }
    }
}
