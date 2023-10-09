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

package io.renku.eventsqueue

import cats.data.Kleisli
import cats.effect._
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.db.{PostgresContainer, TestDbConfig}
import natchez.Trace.Implicits.noop
import org.scalatest.Suite
import skunk._

trait ContainerDB extends ForAllTestContainer { self: Suite =>

  private val initialDBConfig = TestDbConfig.create[TestDB]

  override val container: PostgreSQLContainer = PostgresContainer.container(initialDBConfig)

  lazy val dbConfig: DBConfig[TestDB] =
    DBConfig(
      Refined.unsafeApply(container.host),
      Refined.unsafeApply(container.container.getMappedPort(initialDBConfig.port.value)),
      initialDBConfig.name,
      initialDBConfig.user,
      initialDBConfig.pass,
      connectionPool = 1
    )

  implicit lazy val sessionResource: TestDB.SessionResource[IO] = io.renku.db.SessionResource[IO, TestDB](
    Session.single(
      host = dbConfig.host,
      port = dbConfig.port,
      user = dbConfig.user,
      database = dbConfig.name,
      password = Some(dbConfig.pass)
    )
  )

  def execute[O](query: Kleisli[IO, Session[IO], O]): IO[O] =
    sessionResource.useK(query)
}
