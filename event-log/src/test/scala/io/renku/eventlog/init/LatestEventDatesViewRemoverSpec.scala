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

package io.renku.eventlog.init

import cats.effect.IO
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Info
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._
import skunk.codec.all._

class LatestEventDatesViewRemoverSpec extends AnyWordSpec with DbInitSpec with should.Matchers {

  protected override val migrationsToRun: List[Migration] = Nil

  "run" should {

    "not create a new 'project_latest_event_date' view if it does not exist" in new TestCase {

      viewExists("project_latest_event_date") shouldBe false

      viewCreator.run().unsafeRunSync() shouldBe ((): Unit)

      viewExists("project_latest_event_date") shouldBe false

      logger.loggedOnly(Info("'project_latest_event_date' view dropped"))
    }

    "delete the 'project_latest_event_date' view if it exists" in new TestCase {

      createView()

      viewExists("project_latest_event_date") shouldBe true

      viewCreator.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'project_latest_event_date' view dropped"))
    }
  }

  private trait TestCase {
    val logger      = TestLogger[IO]()
    val viewCreator = new LatestEventDatesViewRemoverImpl[IO](transactor, logger)
  }

  private def createView(): Unit = execute { session =>
    val query: Command[Void] = sql"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS project_latest_event_date AS
    select 1;
    """.command
    session
      .execute(query)
      .map(_ => ())
  }
}
