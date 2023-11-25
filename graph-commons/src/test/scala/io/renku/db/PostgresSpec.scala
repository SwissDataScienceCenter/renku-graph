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
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterAll, Suite}
import skunk.Session

trait PostgresSpec[DB] extends BeforeAndAfterAll {
  self: Suite =>

  def server:      PostgresServer
  def migrations:  Session[IO] => IO[Unit]
  lazy val client: PostgresClient[DB] = new PostgresClient[DB](server, migrations)

  private lazy val specDBName = nonEmptyStrings(minLength = 3, charsGenerator = Gen.alphaNumChar).generateOne

  lazy val configAndSessionResource: Resource[IO, (DBConfig[DB], Session[IO])] =
    client.sessionResource(specDBName)

  lazy val sessionResource: Resource[IO, Session[IO]] =
    configAndSessionResource.map { case (_, s) => s }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    server.start()
  }

  protected override def afterAll(): Unit = {
    server.stop()
    super.afterAll()
  }
}
