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

import java.time.Instant
import java.time.temporal.ChronoUnit.MINUTES

import cats.effect.Bracket
import cats.implicits._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import doobie.implicits._
import eu.timepit.refined.auto._
import io.renku.eventlog.EventStatus.{Processing, RecoverableFailure}
import io.renku.eventlog.statuschange.commands.ProjectPathFinder.findProjectPath
import io.renku.eventlog.{EventLogDB, EventMessage, EventStatus}

import scala.language.higherKinds

final case class ToRecoverableFailure[Interpretation[_]](
    eventId:              CompoundEventId,
    maybeMessage:         Option[EventMessage],
    waitingEventsGauge:   LabeledGauge[Interpretation, projects.Path],
    underProcessingGauge: LabeledGauge[Interpretation, projects.Path],
    now:                  () => Instant = () => Instant.now
)(implicit ME:            Bracket[Interpretation, Throwable])
    extends ChangeStatusCommand[Interpretation] {

  override val status: EventStatus = RecoverableFailure

  override def query: SqlQuery[Int] = SqlQuery(
    sql"""|update event_log
          |set status = $status, execution_date = ${now() plus (10, MINUTES)}, message = $maybeMessage
          |where event_id = ${eventId.id} and project_id = ${eventId.projectId} and status = ${Processing: EventStatus}
          |""".stripMargin.update.run,
    name = "processing->recoverable_fail"
  )

  override def updateGauges(
      updateResult:      UpdateResult
  )(implicit transactor: DbTransactor[Interpretation, EventLogDB]): Interpretation[Unit] = updateResult match {
    case UpdateResult.Updated =>
      for {
        path <- findProjectPath(eventId)
        _    <- waitingEventsGauge increment path
        _    <- underProcessingGauge decrement path
      } yield ()
    case _ => ME.unit
  }
}
