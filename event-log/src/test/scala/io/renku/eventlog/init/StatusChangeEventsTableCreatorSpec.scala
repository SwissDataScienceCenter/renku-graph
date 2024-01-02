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

class StatusChangeEventsTableCreatorSpec extends AsyncFlatSpec with AsyncIOSpec with DbInitSpec with should.Matchers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] =
    classOf[StatusChangeEventsTableCreator[IO]]

  it should "create the 'status_change_events_queue' table if does not exist" in testDBResource.use { implicit cfg =>
    for {
      _ <- tableCreator.run.assertNoException

      _ <- logger.loggedOnlyF(Info("'status_change_events_queue' table created"))

      _ <- tableCreator.run.assertNoException

      _ <- logger.loggedOnlyF(Info("'status_change_events_queue' table created"),
                              Info("'status_change_events_queue' table exists")
           )
    } yield Succeeded
  }

  it should "create indices for the date and event_type columns" in testDBResource.use { implicit cfg =>
    for {
      _ <- tableCreator.run.assertNoException

      _ <- verifyIndexExists("status_change_events_queue", "idx_date").asserting(_ shouldBe true)
      _ <- verifyIndexExists("status_change_events_queue", "idx_event_type").asserting(_ shouldBe true)
    } yield Succeeded
  }

  private def tableCreator(implicit cfg: DBConfig[EventLogDB]) = new StatusChangeEventsTableCreatorImpl[IO]
}
