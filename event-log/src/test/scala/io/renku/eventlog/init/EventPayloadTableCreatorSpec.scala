/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

class EventPayloadTableCreatorSpec extends AsyncFlatSpec with AsyncIOSpec with DbInitSpec with should.Matchers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] = classOf[EventPayloadTableCreator[IO]]

  it should "fail if the 'event' table does not exist" in testDBResource.use { implicit cfg =>
    for {
      _ <- dropTable("event")
      _ <- tableExists("event").asserting(_ shouldBe false)
      _ <- tableExists("event_payload").asserting(_ shouldBe false)

      _ <- tableCreator.run.assertThrowsWithMessage[Exception](
             "Event table missing; creation of event_payload is not possible"
           )
    } yield Succeeded
  }

  it should "create the 'event_payload' table if it doesn't exist" in testDBResource.use { implicit cfg =>
    for {
      _ <- tableExists("event_payload").asserting(_ shouldBe false)

      _ <- tableCreator.run.assertNoException

      _ <- tableExists("event_payload").asserting(_ shouldBe true)

      _ <- logger.loggedOnlyF(Info("'event_payload' table created"))
    } yield Succeeded
  }

  it should "do nothing if the 'event_payload' table already exists" in testDBResource.use { implicit cfg =>
    for {
      _ <- tableExists("event_payload").asserting(_ shouldBe false)

      _ <- tableCreator.run.assertNoException

      _ <- tableExists("event_payload").asserting(_ shouldBe true)

      _ <- logger.loggedOnlyF(Info("'event_payload' table created"))

      _ <- logger.resetF()

      _ <- tableCreator.run.assertNoException

      _ <- logger.loggedOnlyF(Info("'event_payload' table exists"))
    } yield Succeeded
  }

  it should "create indices for certain columns" in testDBResource.use { implicit cfg =>
    for {
      _ <- tableExists("event_payload").asserting(_ shouldBe false)

      _ <- tableCreator.run.assertNoException

      _ <- tableExists("event_payload").asserting(_ shouldBe true)

      _ <- verifyIndexExists("event_payload", "idx_event_payload_event_id").asserting(_ shouldBe true)
      _ <- verifyIndexExists("event_payload", "idx_event_payload_project_id").asserting(_ shouldBe true)
    } yield Succeeded
  }

  private def tableCreator(implicit cfg: DBConfig[EventLogDB]) = new EventPayloadTableCreatorImpl[IO]
}
