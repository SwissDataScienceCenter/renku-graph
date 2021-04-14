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

import cats.MonadError
import cats.data.{Kleisli, NonEmptyList}
import cats.effect.{Bracket, Sync}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.{EventLogDB, EventMessage}

import java.time.Instant

final case class ToGenerationNonRecoverableFailure[Interpretation[_]: Bracket[*[_], Throwable]](
    eventId:                     CompoundEventId,
    message:                     EventMessage,
    underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
    maybeProcessingTime:         Option[EventProcessingTime],
    now:                         () => Instant = () => Instant.now
) extends ChangeStatusCommand[Interpretation] {

  import ProjectPathFinder.findProjectPath

  override lazy val status: EventStatus = GenerationNonRecoverableFailure

  override def queries: NonEmptyList[SqlQuery[Int]] = NonEmptyList.of(
    SqlQuery(
      sql"""|UPDATE event 
            |SET status = $status, execution_date = ${now()}, message = $message
            |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId} AND status = ${GeneratingTriples: EventStatus}
            |""".stripMargin.update.run,
      name = "generating_triples->generation_non_recoverable_fail"
    )
  )

  override def updateGauges(
      updateResult:      UpdateResult
  )(implicit transactor: DbTransactor[Interpretation, EventLogDB]): Interpretation[Unit] = updateResult match {
    case UpdateResult.Updated => findProjectPath(eventId) flatMap underTriplesGenerationGauge.decrement
    case _                    => ().pure[Interpretation]
  }
}

object ToGenerationNonRecoverableFailure {

  import ChangeStatusRequest.EventOnlyRequest
  import CommandFindingResult.{CommandFound, NotSupported, PayloadMalformed}

  def factory[Interpretation[_]: Sync](
      underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path]
  )(implicit
      ME: MonadError[Interpretation, Throwable]
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
