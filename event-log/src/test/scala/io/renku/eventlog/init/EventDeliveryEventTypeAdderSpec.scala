/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.EventLogDB
import io.renku.interpreters.TestLogger.Level.Info
import org.scalatest.Succeeded
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class EventDeliveryEventTypeAdderSpec extends AsyncFlatSpec with AsyncIOSpec with DbInitSpec with should.Matchers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] = classOf[EventDeliveryEventTypeAdder[IO]]

  it should "do nothing if the 'event_type_id' collumn already exists" in testDBResource.use { implicit cfg =>
    for {
      _ <- eventTypeAdder.run.assertNoException

      _ <- verifyColumnExists("event_delivery", "event_type_id").asserting(_ shouldBe true)

      _ <- logger.resetF()

      _ <- eventTypeAdder.run.assertNoException

      _ <- logger.loggedOnlyF(Info("'event_type_id' column adding skipped"))
    } yield Succeeded
  }

  it should "add the 'event_type_id' column if does not exist and create the indices and unique keys" in testDBResource
    .use { implicit cfg =>
      for {
        _ <- verifyColumnExists("event_delivery", "event_type_id").asserting(_ shouldBe false)

        _ <- eventTypeAdder.run.assertNoException

        _ <- verifyColumnExists("event_delivery", "event_type_id").asserting(_ shouldBe true)

        _ <- verifyConstraintExists("event_delivery", "event_delivery_pkey").asserting(_ shouldBe false)
        _ <- verifyConstraintExists("event_delivery", "event_project_id_unique").asserting(_ shouldBe true)
        _ <- verifyConstraintExists("event_delivery", "project_event_type_id_unique").asserting(_ shouldBe true)

        _ <- logger.loggedOnlyF(Info("'event_type_id' column added"))
      } yield Succeeded
    }

  private def eventTypeAdder(implicit cfg: DBConfig[EventLogDB]) = new EventDeliveryEventTypeAdderImpl[IO]
}
