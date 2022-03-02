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

package io.renku.tokenrepository.repository.init

import cats.data.Kleisli
import cats.syntax.all._
import io.renku.graph.model.projects.{Id, Path}
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.InMemoryProjectsTokensDb
import org.scalatest.{BeforeAndAfter, Suite}
import skunk.codec.all.{int4, name, varchar}
import skunk.data.Completion
import skunk.implicits._
import skunk.{Query, Void}

import scala.language.reflectiveCalls

trait DbInitSpec extends InMemoryProjectsTokensDb with DbMigrations with BeforeAndAfter {
  self: Suite with IOSpec =>

  protected val migrationsToRun: List[Migration]

  before {
    findAllTables() foreach dropTable
    migrationsToRun.map(_.run()).sequence.unsafeRunSync()
  }

  private def findAllTables(): List[String] = execute {
    Kleisli { session =>
      val query: Query[Void, String] = sql"""
          SELECT DISTINCT tablename FROM pg_tables
          WHERE schemaname != 'pg_catalog'
            AND schemaname != 'information_schema'""".query(name)
      session.execute(query)
    }
  }

  protected def findToken(projectId: Id): Option[String] = sessionResource
    .useK {
      val query: Query[Int, String] = sql"select token from projects_tokens where project_id = $int4"
        .query(varchar)
      Kleisli(_.prepare(query).use(_.option(projectId.value)))
    }
    .unsafeRunSync()

  protected def findToken(projectPath: Path): Option[String] = sessionResource
    .useK {
      val query: Query[String, String] = sql"select token from projects_tokens where project_path = $varchar"
        .query(varchar)
      Kleisli(_.prepare(query).use(_.option(projectPath.value)))
    }
    .unsafeRunSync()

  protected lazy val assureInserted: Completion => Unit = {
    case Completion.Insert(1) => ()
    case _                    => fail("insertion problem")
  }
}
