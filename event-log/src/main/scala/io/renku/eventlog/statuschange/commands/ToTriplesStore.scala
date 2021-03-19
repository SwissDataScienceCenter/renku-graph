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
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.statuschange.ChangeStatusRequest.EventOnlyRequest
import io.renku.eventlog.statuschange.CommandFindingResult.{CommandFound, NotSupported, PayloadMalformed}
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath
import io.renku.eventlog.statuschange.{ChangeStatusRequest, CommandFindingResult}

import java.time.Instant

final case class ToTriplesStore[Interpretation[_]](
    eventId:                         CompoundEventId,
    underTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    maybeProcessingTime:             Option[EventProcessingTime],
    now:                             () => Instant = () => Instant.now
)(implicit ME:                       Bracket[Interpretation, Throwable])
    extends ChangeStatusCommand[Interpretation] {

  override lazy val status: EventStatus = TriplesStore

  override def queries: NonEmptyList[SqlQuery[Int]] = NonEmptyList.of(
    SqlQuery(
      query = upsertEventStatus,
      name = "transforming_triples->triples_store"
    )
  )

  private lazy val upsertEventStatus =
    sql"""|UPDATE event
          |SET status = $status, execution_date = ${now()}
          |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId} AND status = ${TransformingTriples: EventStatus}
          |""".stripMargin.update.run

  override def updateGauges(
      updateResult:      UpdateResult
  )(implicit transactor: DbTransactor[Interpretation, EventLogDB]): Interpretation[Unit] = updateResult match {
    case UpdateResult.Updated => findProjectPath(eventId) flatMap underTriplesTransformationGauge.decrement
    case _                    => ME.unit
  }
}

private[statuschange] object ToTriplesStore {
  def factory[Interpretation[_]: Sync](
      underTriplesTransformationGauge: LabeledGauge[Interpretation, projects.Path]
  )(implicit
      ME: MonadError[Interpretation, Throwable]
  ): Kleisli[Interpretation, ChangeStatusRequest, CommandFindingResult] = Kleisli.fromFunction {
    case EventOnlyRequest(eventId, TriplesStore, someTime @ Some(_), _) =>
      CommandFound(ToTriplesStore[Interpretation](eventId, underTriplesTransformationGauge, someTime))
    case EventOnlyRequest(_, TriplesStore, None, _) => PayloadMalformed("No processing time provided")
    case _                                          => NotSupported
  }
}
