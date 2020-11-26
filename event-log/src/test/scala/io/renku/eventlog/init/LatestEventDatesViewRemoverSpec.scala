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

package io.renku.eventlog.init

import cats.effect.IO
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import doobie.implicits._
import io.renku.eventlog.InMemoryEventLogDbSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class LatestEventDatesViewRemoverSpec extends AnyWordSpec with InMemoryEventLogDbSpec with should.Matchers {

  "run" should {

    "not create a new 'project_latest_event_date' view if it does not exist" in new TestCase {

      tableExists("project_latest_event_date") shouldBe false

      viewCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("project_latest_event_date") shouldBe false

      logger.loggedOnly(Info("'project_latest_event_date' view dropped"))
    }

    "delete the 'project_latest_event_date' view if it exists" in new TestCase {

      createView()

      tableExists("project_latest_event_date") shouldBe true

      viewCreator.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'project_latest_event_date' view dropped"))
    }
  }

  private trait TestCase {
    val logger      = TestLogger[IO]()
    val viewCreator = new LatestEventDatesViewRemoverImpl[IO](transactor, logger)
  }

  private def createView(): Unit = execute {
    sql"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS project_latest_event_date AS
    select
      project_id,
      project_path,
      MAX(event_date) latest_event_date
    from event_log
    group by project_id, project_path;
    """.update.run.map(_ => ())
  }
}
