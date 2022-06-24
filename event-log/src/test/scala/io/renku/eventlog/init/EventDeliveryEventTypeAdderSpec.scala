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
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDeliveryEventTypeAdderSpec
    extends AnyWordSpec
    with IOSpec
    with DbInitSpec
    with should.Matchers
    with Eventually
    with IntegrationPatience {

  protected[init] override lazy val migrationsToRun: List[DbMigrator[IO]] = allMigrations.takeWhile {
    case _: EventDeliveryEventTypeAdder[IO] => false
    case _ => true
  }

  "run" should {

    "do nothing if the 'event_type_id' collumn already exists" in new TestCase {

      eventTypeAdder.run().unsafeRunSync() shouldBe ()

      verifyColumnExists("event_delivery", "event_type_id") shouldBe true

      logger.reset()

      eventTypeAdder.run().unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("'event_type_id' column adding skipped"))
    }

    "add the 'event_type_id' column if does not exist and create the indices and unique keys" in new TestCase {

      verifyColumnExists("event_delivery", "event_type_id") shouldBe false

      eventTypeAdder.run().unsafeRunSync() shouldBe ()

      verifyColumnExists("event_delivery", "event_type_id") shouldBe true

      verifyConstraintExists("event_delivery", "event_delivery_pkey")          shouldBe false
      verifyConstraintExists("event_delivery", "event_project_id_unique")      shouldBe true
      verifyConstraintExists("event_delivery", "project_event_type_id_unique") shouldBe true

      eventually {
        logger.loggedOnly(Info("'event_type_id' column added"))
      }
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val eventTypeAdder = new EventDeliveryEventTypeAdderImpl[IO]
  }
}
