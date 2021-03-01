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

package io.renku.eventlog.subscriptions.zombieevents

import cats.Monad
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.subscriptions.EventFinder

private class ZombieEventFinder[Interpretation[_]: Monad](
    longProcessingEventsFinder: EventFinder[Interpretation, ZombieEvent]
) extends EventFinder[Interpretation, ZombieEvent] {
  override def popEvent(): Interpretation[Option[ZombieEvent]] = for {
    maybeEvent <- longProcessingEventsFinder.popEvent()
  } yield maybeEvent
}

private object ZombieEventFinder {

  def apply(
      transactor:          DbTransactor[IO, EventLogDB],
      queriesExecTimes:    LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift: ContextShift[IO]): IO[EventFinder[IO, ZombieEvent]] = for {
    longProcessingEventFinder <- LongProcessingEventFinder(transactor, queriesExecTimes)
  } yield new ZombieEventFinder[IO](longProcessingEventFinder)
}
