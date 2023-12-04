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

package io.renku.db

import cats.effect.{IO, Resource}
import io.renku.db.DBConfigProvider.DBConfig
import org.scalatest.{BeforeAndAfterAll, Suite}
import skunk.Session

trait PostgresSpec[DB] extends BeforeAndAfterAll {
  self: Suite =>

  def server:      PostgresServer
  def migrations:  SessionResource[IO, DB] => IO[Unit]
  lazy val client: PostgresClient[DB] = new PostgresClient[DB](server, migrations)

  def testDBResource: Resource[IO, DBConfig[DB]] =
    client.randomizedDBResource(prefix = getClass.getSimpleName.toLowerCase)

  lazy val sessionResource: DBConfig[DB] => Resource[IO, Session[IO]] =
    client.sessionResource

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    server.start()
  }

  protected override def afterAll(): Unit = {
    server.stop()
    super.afterAll()
  }
}
