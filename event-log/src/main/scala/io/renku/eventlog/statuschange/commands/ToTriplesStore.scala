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

import cats.effect.Bracket
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath

import java.time.Instant

final case class ToTriplesStore[Interpretation[_]](
    eventId:                     CompoundEventId,
    underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
    now:                         () => Instant = () => Instant.now
)(implicit ME:                   Bracket[Interpretation, Throwable])
    extends ChangeStatusCommand[Interpretation] {

  override lazy val status: EventStatus = TriplesStore
// TODO temporary status change from TriplesGenerated to triples store in the end only TransformingTriples can be transformed to TriplesStore
  override def query: SqlQuery[Int] = SqlQuery(
    sql"""|UPDATE event
          |SET status = $status, execution_date = ${now()}
          |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId} AND (status = ${TriplesGenerated: EventStatus} OR status = ${TransformingTriples: EventStatus})
          |""".stripMargin.update.run,
    name = "triples_generated-transforming_triples->triples_store"
  )

  override def updateGauges(
      updateResult:      UpdateResult
  )(implicit transactor: DbTransactor[Interpretation, EventLogDB]): Interpretation[Unit] = updateResult match {
    case UpdateResult.Updated => findProjectPath(eventId) flatMap underTriplesGenerationGauge.decrement
    case _                    => ME.unit
  }
}
