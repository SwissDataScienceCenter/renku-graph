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

package io.renku.eventlog

import cats.effect.IO
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.db.{PostgresServer, PostgresSpec, SessionResource}
import io.renku.eventlog.init.DbInitializer
import io.renku.interpreters.TestLogger
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Suite

trait EventLogPostgresSpec extends PostgresSpec[EventLogDB] with TypeSerializers with EventLogDBProvisioning {
  self: Suite with AsyncMockFactory =>

  lazy val server: PostgresServer = EventLogPostgresServer

  implicit val logger: TestLogger[IO] = TestLogger()

  lazy val migrations: SessionResource[IO, EventLogDB] => IO[Unit] = { implicit sr =>
    DbInitializer.migrations[IO].traverse_(_.run)
  }

  implicit def moduleSessionResource(implicit cfg: DBConfig[EventLogDB]): EventLogDB.SessionResource[IO] =
    io.renku.db.SessionResource[IO, EventLogDB](sessionResource(cfg), cfg)
}
