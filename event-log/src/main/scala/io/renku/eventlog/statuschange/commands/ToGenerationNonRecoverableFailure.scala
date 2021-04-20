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

package io.renku.eventlog.statuschange
package commands

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.{Async, Bracket}
import cats.syntax.all._
import ch.datascience.db.SqlQuery
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import eu.timepit.refined.auto._
import io.renku.eventlog.{EventMessage, ExecutionDate}
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

final case class ToGenerationNonRecoverableFailure[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    eventId:                     CompoundEventId,
    message:                     EventMessage,
    underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
    maybeProcessingTime:         Option[EventProcessingTime],
    now:                         () => Instant = () => Instant.now
) extends ChangeStatusCommand[Interpretation] {

  import ProjectPathFinder.findProjectPath

  override lazy val status: EventStatus = GenerationNonRecoverableFailure

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
          .use(_.execute(status ~ ExecutionDate(now()) ~ message ~ eventId.id ~ eventId.projectId ~ GeneratingTriples))
          .map {
            case Completion.Update(n) => n
            case completion =>
              throw new RuntimeException(
                s"generating_triples->generation_non_recoverable_fail time query failed with completion status $completion"
              )
          } // TODO Verify
      },
      name = "generating_triples->generation_non_recoverable_fail"
    )
  )

  override def updateGauges(
      updateResult: UpdateResult
  ): Kleisli[Interpretation, Session[Interpretation], Unit] = updateResult match {
    case UpdateResult.Updated =>
      findProjectPath(eventId).flatMap(label => Kleisli.liftF(underTriplesGenerationGauge.decrement(label)))
    case _ => Kleisli.pure(())
  }
}

object ToGenerationNonRecoverableFailure {

  import ChangeStatusRequest.EventOnlyRequest
  import CommandFindingResult.{CommandFound, NotSupported, PayloadMalformed}

  def factory[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path]
  ): Kleisli[Interpretation, ChangeStatusRequest, CommandFindingResult] = Kleisli.fromFunction {
    case EventOnlyRequest(eventId, GenerationNonRecoverableFailure, maybeProcessingTime, Some(message)) =>
      CommandFound(
        ToGenerationNonRecoverableFailure[Interpretation](eventId,
                                                          message,
                                                          underTriplesGenerationGauge,
                                                          maybeProcessingTime
        )
      )
    case EventOnlyRequest(_, GenerationNonRecoverableFailure, _, None) => PayloadMalformed("No message provided")
    case _                                                             => NotSupported
  }
}
