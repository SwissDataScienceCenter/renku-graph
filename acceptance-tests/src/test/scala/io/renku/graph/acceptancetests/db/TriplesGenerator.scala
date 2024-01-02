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

package io.renku.graph.acceptancetests.db

import cats.effect.IO
import io.renku.db.DBConfigProvider
import io.renku.triplesgenerator.{TgDB, TgDbConfigProvider}
import org.typelevel.log4cats.Logger

import scala.util.Try

object TriplesGenerator {

  private val dbConfig: DBConfigProvider.DBConfig[TgDB] =
    new TgDbConfigProvider[Try].get().fold(throw _, identity)

  def startDB()(implicit logger: Logger[IO]): IO[Unit] = for {
    _ <- PostgresDB.startPostgres
    _ <- PostgresDB.initializeDatabase(dbConfig)
    _ <- logger.info("triples_generator DB started")
  } yield ()
}
