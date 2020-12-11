/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.effect.{Bracket, IO}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TriplesGenerated}
import ch.datascience.graph.model.events.{CompoundEventId, EventStatus}
import ch.datascience.graph.model.projects.SchemaVersion
import ch.datascience.graph.model.{events, projects}
import ch.datascience.metrics.LabeledGauge
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.{EventLogDB, EventPayload}
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath
import io.renku.eventlog.statuschange.commands.UpdateResult.Updated

import java.time.Instant

private[statuschange] final case class ToTriplesGenerated[Interpretation[_]](
    eventId:                     CompoundEventId,
    payload:                     EventPayload,
    schemaVersion:               SchemaVersion,
    underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
    awaitingTransformationGauge: LabeledGauge[Interpretation, projects.Path],
    now:                         () => Instant = () => Instant.now
)(implicit ME:                   Bracket[Interpretation, Throwable])
    extends ChangeStatusCommand[Interpretation] {
  override lazy val status: events.EventStatus = TriplesGenerated

  override def query: SqlQuery[Int] = SqlQuery(
    query = updateStatus.flatMap { result =>
      mapResult(result) match {
        case Updated => upsertEventPayload
        case _       => result.pure[ConnectionIO]
      }
    },
    name = "generating_triples->triples_generated"
  )

  private lazy val updateStatus = sql"""|UPDATE event
                                        |SET status = $status, execution_date = ${now()}
                                        |WHERE event_id = ${eventId.id} AND project_id = ${eventId.projectId} AND status = ${GeneratingTriples: EventStatus};
                                        |""".stripMargin.update.run

  private lazy val upsertEventPayload = sql"""|INSERT INTO
                                              |event_payload (event_id, project_id, payload, schema_version)
                                              |VALUES (${eventId.id},  ${eventId.projectId}, $payload, $schemaVersion)
                                              |ON CONFLICT (event_id, project_id, schema_version)
                                              |DO UPDATE SET payload = EXCLUDED.payload;
                                              |""".stripMargin.update.run

  override def updateGauges(updateResult: UpdateResult)(implicit
      transactor:                         DbTransactor[Interpretation, EventLogDB]
  ): Interpretation[Unit] = updateResult match {
    case UpdateResult.Updated =>
      for {
        path <- findProjectPath(eventId)
        _    <- awaitingTransformationGauge increment path
        _    <- underTriplesGenerationGauge decrement path
      } yield ()
    case _ => ME.unit
  }
}
