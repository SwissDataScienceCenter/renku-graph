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

package io.renku.eventlog.events.categories.zombieevents

import cats.effect.{Bracket, IO}
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import io.renku.eventlog.EventLogDB

import java.time.Instant

private trait EventStatusUpdater[Interpretation[_]] {
  def changeStatus(event: ZombieEvent): Interpretation[Unit]
}

private class EventStatusUpdaterImpl(
    transactor:         DbTransactor[IO, EventLogDB],
    waitingEventsGauge: LabeledGauge[IO, projects.Path],
    queriesExecTimes:   LabeledHistogram[IO, SqlQuery.Name],
    now:                () => Instant = () => Instant.now
)(implicit ME:          Bracket[IO, Throwable])
    extends DbClient(Some(queriesExecTimes))
    with EventStatusUpdater[IO] {

  override def changeStatus(event: ZombieEvent): IO[Unit] = ???
}

private object EventStatusUpdater {
  import cats.effect.IO

  def apply(transactor:         DbTransactor[IO, EventLogDB],
            waitingEventsGauge: LabeledGauge[IO, projects.Path],
            queriesExecTimes:   LabeledHistogram[IO, SqlQuery.Name]
  ): IO[EventStatusUpdater[IO]] = IO {
    new EventStatusUpdaterImpl(transactor, waitingEventsGauge, queriesExecTimes)
  }
}
