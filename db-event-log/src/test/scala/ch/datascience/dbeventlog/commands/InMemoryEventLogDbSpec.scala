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

import ch.datascience.db.DbSpec
import ch.datascience.dbeventlog._
import ch.datascience.graph.model.events._
import doobie.implicits._
import doobie.util.fragment.Fragment
import org.scalatest.TestSuite

trait InMemoryEventLogDbSpec extends DbSpec with InMemoryEventLogDb {
  self: TestSuite =>

  protected def initDb(): Unit = execute {
    sql"""
         |CREATE TABLE IF NOT EXISTS event_log(
         | event_id varchar NOT NULL,
         | project_id int4 NOT NULL,
         | status varchar NOT NULL,
         | created_date timestamp NOT NULL,
         | execution_date timestamp NOT NULL,
         | event_date timestamp NOT NULL,
         | event_body text NOT NULL,
         | message varchar,
         | PRIMARY KEY (event_id, project_id)
         |);
      """.stripMargin.update.run.map(_ => ())
  }

  protected def prepareDbForTest(): Unit = execute {
    sql"TRUNCATE TABLE event_log".update.run.map(_ => ())
  }

  protected def storeEvent(commitEventId: CommitEventId,
                           eventStatus:   EventStatus,
                           executionDate: ExecutionDate,
                           eventDate:     CommittedDate,
                           eventBody:     EventBody,
                           createdDate:   CreatedDate = CreatedDate(Instant.now)): Unit = execute {
    sql"""insert into 
         |event_log (event_id, project_id, status, created_date, execution_date, event_date, event_body) 
         |values (${commitEventId.id}, ${commitEventId.projectId}, $eventStatus, $createdDate, $executionDate, $eventDate, $eventBody)
      """.stripMargin.update.run.map(_ => ())
  }

  // format: off
  protected def findEvents(status:  EventStatus,
                           orderBy: Fragment = fr"created_date asc"): List[(CommitEventId, ExecutionDate)] =
    execute {
      (fr"""select event_id, project_id, execution_date
            from event_log 
            where status = $status
            order by """ ++ orderBy)
        .query[(CommitEventId, ExecutionDate)]
        .to[List]
    }
  // format: on
}
