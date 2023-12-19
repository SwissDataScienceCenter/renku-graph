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

class SubscriptionCategorySyncTimeTableCreatorSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with DbInitSpec
    with should.Matchers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] =
    classOf[SubscriptionCategorySyncTimeTableCreator[IO]]

  it should "do nothing if the 'subscription_category_sync_time' table already exists" in testDBResource.use {
    implicit cfg =>
      for {
        _ <- tableExists("subscription_category_sync_time").asserting(_ shouldBe false)

        _ <- tableCreator.run.assertNoException

        _ <- tableExists("subscription_category_sync_time").asserting(_ shouldBe true)

        _ <- logger.loggedOnlyF(Info("'subscription_category_sync_time' table created"))

        _ <- logger.resetF()

        _ <- tableCreator.run.assertNoException

        _ <- logger.loggedOnlyF(Info("'subscription_category_sync_time' table exists"))
      } yield Succeeded
  }

  it should "create indices for certain columns" in testDBResource.use { implicit cfg =>
    for {
      _ <- tableExists("subscription_category_sync_time").asserting(_ shouldBe false)

      _ <- tableCreator.run.assertNoException

      _ <- tableExists("subscription_category_sync_time").asserting(_ shouldBe true)

      _ <- verifyIndexExists("subscription_category_sync_time", "idx_project_id").asserting(_ shouldBe true)
      _ <- verifyIndexExists("subscription_category_sync_time", "idx_category_name").asserting(_ shouldBe true)
      _ <- verifyIndexExists("subscription_category_sync_time", "idx_last_synced").asserting(_ shouldBe true)
    } yield Succeeded
  }

  private def tableCreator(implicit cfg: DBConfig[EventLogDB]) = new SubscriptionCategorySyncTimeTableCreatorImpl[IO]
}
