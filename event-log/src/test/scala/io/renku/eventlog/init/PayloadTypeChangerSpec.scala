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

class PayloadTypeChangerSpec extends AsyncFlatSpec with AsyncIOSpec with DbInitSpec with should.Matchers {

  protected[init] override val runMigrationsUpTo: Class[_ <: DbMigrator[IO]] = classOf[PayloadTypeChanger[IO]]

  it should "do nothing if the payload is already a bytea" in testDBResource.use { implicit cfg =>
    for {
      _ <- tableExists("event_payload").asserting(_ shouldBe true)
      _ <- verifyExists("event_payload", "payload", "text").asserting(_ shouldBe true)

      _ <- tableRefactor.run.assertNoException

      _ <- verifyExists("event_payload", "payload", "bytea").asserting(_ shouldBe true)
      _ <- verifyColumnExists("event_payload", "schema_version").asserting(_ shouldBe false)

      _ <- tableRefactor.run.assertNoException

      _ <- verifyExists("event_payload", "payload", "bytea").asserting(_ shouldBe true)
      _ <- verifyColumnExists("event_payload", "schema_version").asserting(_ shouldBe false)

      _ <- logger.loggedOnlyF(Info("event_payload.payload already in bytea type"))
    } yield Succeeded
  }

  private def tableRefactor(implicit cfg: DBConfig[EventLogDB]) = new PayloadTypeChangerImpl[IO]
}
