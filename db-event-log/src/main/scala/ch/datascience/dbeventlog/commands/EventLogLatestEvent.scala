/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.dbeventlog.commands

import cats.MonadError
import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.db.TransactorProvider
import ch.datascience.dbeventlog.IOTransactorProvider
import ch.datascience.graph.model.events.{CommitId, ProjectId}
import doobie.implicits._

import scala.language.higherKinds

class EventLogLatestEvent[Interpretation[_]](
    transactorProvider: TransactorProvider[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  def findYoungestEventInLog(projectId: ProjectId): Interpretation[Option[CommitId]] =
    for {
      transactor <- transactorProvider.transactor
      maybeEventId <- sql"""
                           |select event_id
                           |from event_log
                           |where project_id = $projectId
                           |order by event_date desc
                           |limit 1""".stripMargin
                       .query[CommitId]
                       .option
                       .transact(transactor)
    } yield maybeEventId
}

class IOEventLogLatestEvent(
    implicit contextShift: ContextShift[IO]
) extends EventLogLatestEvent[IO](new IOTransactorProvider)
