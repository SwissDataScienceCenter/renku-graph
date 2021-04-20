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
import io.renku.eventlog.ExecutionDate
import io.renku.eventlog.statuschange.ChangeStatusRequest.EventOnlyRequest
import io.renku.eventlog.statuschange.CommandFindingResult.{CommandFound, NotSupported, PayloadMalformed}
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath
import io.renku.eventlog.statuschange.{ChangeStatusRequest, CommandFindingResult}
import skunk._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant

final case class ToTriplesStore[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    eventId:                         CompoundEventId,
    underTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    maybeProcessingTime:             Option[EventProcessingTime],
    now:                             () => Instant = () => Instant.now
) extends ChangeStatusCommand[Interpretation] {

  override lazy val status: EventStatus = TriplesStore

  override def queries: NonEmptyList[SqlQuery[Interpretation, Int]] = NonEmptyList.of(
    SqlQuery(
      query = upsertEventStatus,
      name = "transforming_triples->triples_store"
    )
  )

  private lazy val upsertEventStatus = Kleisli[Interpretation, Session[Interpretation], Int] { session =>
    val query: Command[EventStatus ~ ExecutionDate ~ EventId ~ projects.Id ~ EventStatus] =
      sql"""UPDATE event
            SET status = $eventStatusEncoder, execution_date = $executionDateEncoder
            WHERE event_id = $eventIdEncoder AND project_id = $projectIdEncoder AND status = $eventStatusEncoder
            """.command
    session
      .prepare(query)
      .use(_.execute(status ~ ExecutionDate(now()) ~ eventId.id ~ eventId.projectId ~ TransformingTriples))
      .map {
        case Completion.Update(n) => n
        case completion =>
          throw new RuntimeException(
            s"transforming_triples->triples_store time query failed with completion status $completion"
          )
      }
  }

  override def updateGauges(
      updateResult: UpdateResult
  ): Kleisli[Interpretation, Session[Interpretation], Unit] = updateResult match {
    case UpdateResult.Updated =>
      findProjectPath(eventId).flatMap(label => Kleisli.liftF(underTriplesTransformationGauge.decrement(label)))
    case _ => Kleisli.pure(())
  }
}

private[statuschange] object ToTriplesStore {
  def factory[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      underTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path]
  ): Kleisli[Interpretation, ChangeStatusRequest, CommandFindingResult] = Kleisli.fromFunction {
    case EventOnlyRequest(eventId, TriplesStore, someTime @ Some(_), _) =>
      CommandFound(ToTriplesStore[Interpretation](eventId, underTriplesTransformationGauge, someTime))
    case EventOnlyRequest(_, TriplesStore, None, _) => PayloadMalformed("No processing time provided")
    case _                                          => NotSupported
  }
}
