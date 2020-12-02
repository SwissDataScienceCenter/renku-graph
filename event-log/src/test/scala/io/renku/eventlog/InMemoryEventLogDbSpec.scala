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

package io.renku.eventlog

import java.time.Instant

import ch.datascience.db.DbSpec
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Path
import doobie.implicits._
import doobie.util.fragment.Fragment
import org.scalatest.TestSuite

trait InMemoryEventLogDbSpec
    extends DbSpec
    with InMemoryEventLogDb
    with EventLogDataProvisioning
    with EventLogDataFetching {
  self: TestSuite =>

  protected def initDb(): Unit = {
    execute {
      sql"""|CREATE TABLE IF NOT EXISTS event(
            | event_id varchar NOT NULL,
            | project_id int4 NOT NULL,
            | status varchar NOT NULL,
            | created_date timestamp NOT NULL,
            | execution_date timestamp NOT NULL,
            | event_date timestamp NOT NULL,
            | batch_date timestamp NOT NULL,
            | event_body text NOT NULL,
            | message varchar,
            | PRIMARY KEY (event_id, project_id)
            |);
       """.stripMargin.update.run.map(_ => ())
    }
    execute {
      sql"""|CREATE TABLE IF NOT EXISTS project(
            |project_id        int4      NOT NULL,
            |project_path      VARCHAR   NOT NULL,
            |latest_event_date timestamp NOT NULL,
            |PRIMARY KEY (project_id)
            |);
    """.stripMargin.update.run.map(_ => ())
    }
  }

  protected def prepareDbForTest(): Unit = Tables.all.foreach { tableName =>
    execute {
      Fragment.const(s"TRUNCATE TABLE $tableName").update.run.map(_ => ())
    }
  }
}
