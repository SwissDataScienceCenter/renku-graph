/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CleanUpEventsTableCreatorSpec extends AnyWordSpec with IOSpec with DbInitSpec with should.Matchers {

  protected[init] override lazy val migrationsToRun: List[DbMigrator[IO]] = allMigrations.takeWhile {
    case _: CleanUpEventsTableCreatorImpl[IO] => false
    case _ => true
  }

  "run" should {

    "create the 'clean_up_events_queue' table if does not exist" in new TestCase {

      tableCreator.run().unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("'clean_up_events_queue' table created"))

      tableCreator.run().unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("'clean_up_events_queue' table created"), Info("'clean_up_events_queue' table exists"))
    }

    "create indices for the date column" in new TestCase {

      tableCreator.run().unsafeRunSync() shouldBe ()

      verifyIndexExists("clean_up_events_queue", "idx_date")
      verifyIndexExists("clean_up_events_queue", "idx_project_path")
      verifyConstraintExists("clean_up_events_queue", "project_path_unique")
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tableCreator = new CleanUpEventsTableCreatorImpl[IO]
  }
}
