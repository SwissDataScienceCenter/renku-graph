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
import io.renku.eventlog.statuschange.commands.CommandFindingResult.CommandFound
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath
import io.renku.eventlog.subscriptions.EventDelivery
import io.renku.eventlog.{EventLogDB, EventMessage}
import org.http4s.{MediaType, Request}

import java.time.Instant
import java.time.temporal.ChronoUnit.MINUTES

final case class ToGenerationRecoverableFailure[Interpretation[_]](
    eventId:                        CompoundEventId,
    maybeMessage:                   Option[EventMessage],
    awaitingTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
    underTriplesGenerationGauge:    LabeledGauge[Interpretation, projects.Path],
    maybeProcessingTime:            Option[EventProcessingTime],
    eventDelivery:                  EventDelivery[Interpretation, ToGenerationRecoverableFailure[Interpretation]],
    now:                            () => Instant = () => Instant.now
)(implicit ME:                      Bracket[Interpretation, Throwable])
    extends ChangeStatusCommand[Interpretation] {

  override lazy val status: EventStatus = GenerationRecoverableFailure

  override def queries: NonEmptyList[SqlQuery[Int]] = NonEmptyList(
    SqlQuery(
      sql"""|UPDATE event
            |SET status = $status, execution_date = ${now().plus(10, MINUTES)}, message = $maybeMessage
            |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId} AND status = ${GeneratingTriples: EventStatus}
            |""".stripMargin.update.run,
      name = "generating_triples->generation_recoverable_fail"
    ),
    Nil
  )

  override def updateGauges(
      updateResult:      UpdateResult
  )(implicit transactor: DbTransactor[Interpretation, EventLogDB]): Interpretation[Unit] = updateResult match {
    case UpdateResult.Updated =>
      for {
        path <- findProjectPath(eventId)
        _    <- awaitingTriplesGenerationGauge increment path
        _    <- underTriplesGenerationGauge decrement path
      } yield ()
    case _ => ME.unit
  }

  override def updateDelivery(): Interpretation[Unit] = eventDelivery.unregister(eventId)
}

object ToGenerationRecoverableFailure {
  def factory[Interpretation[_]: Sync](
      awaitingTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
      underTriplesGenerationGauge:    LabeledGauge[Interpretation, projects.Path],
      eventDelivery:                  EventDelivery[Interpretation, ToGenerationRecoverableFailure[Interpretation]]
  )(implicit
      ME: MonadError[Interpretation, Throwable]
  ): Kleisli[Interpretation, (CompoundEventId, Request[Interpretation]), CommandFindingResult] =
    Kleisli { case (eventId, request) =>
      when(request, has = MediaType.application.json) {
        {
          for {
            _                   <- request.validate(status = GenerationRecoverableFailure)
            maybeProcessingTime <- request.getProcessingTime
            maybeMessage        <- request.getMessage
          } yield CommandFound(
            ToGenerationRecoverableFailure[Interpretation](
              eventId,
              maybeMessage,
              awaitingTriplesGenerationGauge,
              underTriplesGenerationGauge,
              maybeProcessingTime,
              eventDelivery
            )
          )
        }.merge
      }
    }
}
