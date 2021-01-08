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

package io.renku.eventlog.subscriptions.membersync

import cats.effect.{Bracket, ContextShift, IO}
import ch.datascience.db.{DbClient, DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledHistogram
import io.renku.eventlog.{EventLogDB, TypeSerializers}
import io.renku.eventlog.subscriptions.EventFinder

private class MemberSyncEventFinderImpl(transactor:       DbTransactor[IO, EventLogDB],
                                        queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
)(implicit ME:                                            Bracket[IO, Throwable], contextShift: ContextShift[IO])
    extends DbClient(Some(queriesExecTimes))
    with EventFinder[IO, MemberSyncEvent]
    with TypeSerializers {
  override def popEvent(): IO[Option[MemberSyncEvent]] = ???
}

private object MemberSyncEventsFinder {
  def apply(
      transactor:       DbTransactor[IO, EventLogDB],
      queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name]
  )(implicit ME:        Bracket[IO, Throwable], contextShift: ContextShift[IO]): IO[EventFinder[IO, MemberSyncEvent]] = ???
}
