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

import ch.datascience.generators.Generators.Implicits._
import cats.effect.IO
import ch.datascience.db.DbSpec
import ch.datascience.dbeventlog.DbEventLogGenerators.createdDates
import ch.datascience.dbeventlog._
import ch.datascience.graph.model.events.{CommitId, CommittedDate, ProjectId}
import doobie.implicits._
import doobie.util.transactor.Transactor.Aux

trait InMemoryEventLogDb {
  self: DbSpec =>

  protected def initDb(transactor: Aux[IO, Unit]): Unit =
    sql"""
         |CREATE TABLE IF NOT EXISTS event_log(
         | event_id varchar PRIMARY KEY,
         | project_id int4 NOT NULL,
         | status varchar NOT NULL,
         | created_date timestamp NOT NULL,
         | execution_date timestamp NOT NULL,
         | event_date timestamp NOT NULL,
         | event_body text NOT NULL,
         | message varchar
         |);
      """.stripMargin.update.run
      .transact(transactor)
      .unsafeRunSync()

  protected def prepareDbForTest(transactor: Aux[IO, Unit]): Unit =
    sql"TRUNCATE TABLE event_log".update.run
      .transact(transactor)
      .unsafeRunSync()

  protected def storeEvent(eventId:       CommitId,
                           projectId:     ProjectId,
                           eventStatus:   EventStatus,
                           executionDate: ExecutionDate,
                           eventDate:     CommittedDate,
                           eventBody:     EventBody,
                           createdDate:   CreatedDate = createdDates.generateOne): Unit =
    sql"""insert into 
         |event_log (event_id, project_id, status, created_date, execution_date, event_date, event_body) 
         |values ($eventId, $projectId, $eventStatus, $createdDate, $executionDate, $eventDate, $eventBody)
      """.stripMargin.update.run
      .map(_ => ())
      .transact(transactor)
      .unsafeRunSync()

  protected def findEvent(status: EventStatus): List[(CommitId, ExecutionDate)] =
    sql"""select event_id, execution_date
         |from event_log 
         |where status = $status
         """.stripMargin
      .query[(CommitId, ExecutionDate)]
      .to[List]
      .transact(transactor)
      .unsafeRunSync()
}
