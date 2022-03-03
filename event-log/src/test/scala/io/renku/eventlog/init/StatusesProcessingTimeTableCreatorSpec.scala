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
import skunk.implicits._

class StatusesProcessingTimeTableCreatorSpec extends AnyWordSpec with IOSpec with DbInitSpec with should.Matchers {

  protected[init] override lazy val migrationsToRun: List[DbMigrator[IO]] = allMigrations.takeWhile {
    case _: StatusesProcessingTimeTableCreatorImpl[IO] => false
    case _ => true
  }

  "run" should {

    "do nothing if the 'status_transition_time' table already exists" in new TestCase {

      tableExists("status_processing_time") shouldBe false

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("status_processing_time") shouldBe true

      logger.loggedOnly(Info("'status_processing_time' table created"))

      logger.reset()

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      logger.loggedOnly(Info("'status_processing_time' table exists"))

    }

    "create indices for certain columns" in new TestCase {

      tableExists("status_processing_time") shouldBe false

      tableCreator.run().unsafeRunSync() shouldBe ((): Unit)

      tableExists("status_processing_time") shouldBe true

      verifyTrue(sql"DROP INDEX idx_event_id;".command)
      verifyTrue(sql"DROP INDEX idx_project_id;".command)
      verifyTrue(sql"DROP INDEX idx_status;".command)

    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val tableCreator = new StatusesProcessingTimeTableCreatorImpl[IO]
  }
}
