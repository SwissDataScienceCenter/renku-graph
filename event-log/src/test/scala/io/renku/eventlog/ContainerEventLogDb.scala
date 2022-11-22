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

package io.renku.eventlog

import cats.effect._
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import eu.timepit.refined.api.Refined
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.db.PostgresContainer
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.testtools.IOSpec
import natchez.Trace.Implicits.noop
import org.scalatest.Suite
import skunk._
import eu.timepit.refined.auto._

trait ContainerEventLogDb extends ForAllTestContainer { self: Suite with IOSpec =>
  private val initialDbConfig = new EventLogDbConfigProvider[IO].get().unsafeRunSync()

  override val container: PostgreSQLContainer = PostgresContainer.container(initialDbConfig)

  lazy val dbConfig: DBConfig[EventLogDB] =
    DBConfig(
      Refined.unsafeApply(container.host),
      Refined.unsafeApply(container.container.getMappedPort(initialDbConfig.port.value)),
      "event_log",
      Refined.unsafeApply(initialDbConfig.user.value),
      Refined.unsafeApply(initialDbConfig.pass.value),
      1
    )

  implicit lazy val sessionResource: SessionResource[IO] = new io.renku.db.SessionResource[IO, EventLogDB](
    Session.single(
      host = dbConfig.host,
      port = dbConfig.port,
      user = dbConfig.user,
      database = dbConfig.name,
      password = Some(dbConfig.pass)
    )
  )
}
