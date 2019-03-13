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

import java.time.Instant

import cats.MonadError
import cats.effect.{ContextShift, IO}
import cats.free.Free
import cats.implicits._
import ch.datascience.db.TransactorProvider
import ch.datascience.dbeventlog.{EventBody, EventStatus, IOTransactorProvider}
import ch.datascience.graph.model.events.{CommitId, ProjectId}
import doobie.free.connection
import doobie.implicits._

import scala.language.higherKinds

class EventLogAdd[Interpretation[_]](
    transactorProvider: TransactorProvider[Interpretation],
    now:                () => Instant = Instant.now
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  import ModelReadsAndWrites._

  def storeNewEvent(eventId: CommitId, projectId: ProjectId, eventBody: EventBody): Interpretation[Unit] =
    for {
      transactor <- transactorProvider.transactor
      _ <- sql"select event_id from event_log where event_id = $eventId"
            .query[String]
            .option
            .flatMap {
              case None => insert(eventId, projectId, eventBody)
              case _    => Free.pure(())
            }
            .transact(transactor)
    } yield ()

  private def insert(eventId:   CommitId,
                     projectId: ProjectId,
                     eventBody: EventBody): Free[connection.ConnectionOp, Unit] = {
    val currentTime = now()
    sql"""insert into 
          event_log (event_id, project_id, status, created_date, execution_date, event_body) 
          values ($eventId, $projectId, ${EventStatus.New: EventStatus}, $currentTime, $currentTime, $eventBody)
      """.update.run.map(_ => ())
  }
}

class IOEventLogAdd(implicit contextShift: ContextShift[IO]) extends EventLogAdd[IO](new IOTransactorProvider)
