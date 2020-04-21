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

package io.renku.eventlog.rescheduling

import java.time.Instant

import cats.effect.{Bracket, ContextShift, IO}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import doobie.implicits._
import io.renku.eventlog.EventStatus.New
import io.renku.eventlog.{EventLogDB, EventStatus, TypesSerializers}

import scala.language.higherKinds

private class ReScheduler[Interpretation[_]](
    transactor:         DbTransactor[Interpretation, EventLogDB],
    waitingEventsGauge: LabeledGauge[Interpretation, projects.Path],
    now:                () => Instant = () => Instant.now
)(implicit ME:          Bracket[Interpretation, Throwable])
    extends TypesSerializers {

  def scheduleEventsForProcessing: Interpretation[Unit] =
    for {
      _ <- runUpdate() transact transactor.get
      _ <- waitingEventsGauge.reset
    } yield ()

  private def runUpdate() =
    sql"""|update event_log 
          |set status = ${New: EventStatus}, execution_date = event_date, batch_date = ${now()}, message = NULL
          |""".stripMargin.update.run
}

private class IOReScheduler(
    transactor:          DbTransactor[IO, EventLogDB],
    waitingEventsGauge:  LabeledGauge[IO, projects.Path]
)(implicit contextShift: ContextShift[IO])
    extends ReScheduler[IO](transactor, waitingEventsGauge)
