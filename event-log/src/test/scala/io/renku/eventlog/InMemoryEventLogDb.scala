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

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import com.dimafeng.testcontainers._
import io.renku.db.SessionResource
import io.renku.testtools.IOSpec
import natchez.Trace.Implicits.noop
import org.scalatest.Suite
import org.testcontainers.utility.DockerImageName
import skunk._
import skunk.codec.all._
import skunk.implicits._

trait InMemoryEventLogDb extends ForAllTestContainer with TypeSerializers {
  self: Suite with IOSpec =>

  private val dbConfig = new EventLogDbConfigProvider[IO].get().unsafeRunSync()

  override val container: PostgreSQLContainer = PostgreSQLContainer(
    dockerImageNameOverride = DockerImageName.parse("postgres:11.11-alpine"),
    databaseName = dbConfig.name.value,
    username = dbConfig.user.value,
    password = dbConfig.pass.value
  )

  lazy val sessionResource: SessionResource[IO, EventLogDB] = new SessionResource[IO, EventLogDB](
    Session.single(
      host = container.host,
      port = container.container.getMappedPort(dbConfig.port.value),
      user = dbConfig.user.value,
      database = dbConfig.name.value,
      password = Some(dbConfig.pass.value)
    )
  )

  def execute[O](query: Kleisli[IO, Session[IO], O]): O =
    sessionResource.useK(query).unsafeRunSync()

  def verifyTrue(sql: Command[Void]): Unit = execute[Unit](Kleisli(session => session.execute(sql).void))

  def verifyIndexExists(tableName: String, indexName: String): Boolean = execute[Boolean](Kleisli { session =>
    val query: Query[String ~ String, Boolean] =
      sql"""SELECT EXISTS(SELECT indexname FROM pg_indexes WHERE tablename = $varchar AND indexname = $varchar)"""
        .query(bool)

    session.prepare(query).use(_.unique(tableName ~ indexName)).recover(_ => false)
  })

  def verify(table: String, column: String, hasType: String): Boolean = execute[Boolean] {
    Kleisli { session =>
      val query: Query[String ~ String, String] =
        sql"""SELECT data_type FROM information_schema.columns WHERE
         table_name = $varchar AND column_name = $varchar;""".query(varchar)
      session
        .prepare(query)
        .use(_.unique(table ~ column))
        .map(dataType => dataType == hasType)
        .recover { case _ => false }
    }
  }

  def verifyColumnExists(table: String, column: String): Boolean = execute[Boolean] {
    Kleisli { session =>
      val query: Query[String ~ String, Boolean] =
        sql"""SELECT EXISTS (
                SELECT *
                FROM information_schema.columns 
                WHERE table_name = $varchar AND column_name = $varchar
              )""".query(bool)
      session
        .prepare(query)
        .use(_.unique(table ~ column))
        .recover { case _ => false }
    }
  }

  def verifyConstraintExists(table: String, constraintName: String): Boolean = execute[Boolean] {
    Kleisli { session =>
      val query: Query[String ~ String, Boolean] =
        sql"""SELECT EXISTS (
                 SELECT * 
                 FROM information_schema.constraint_column_usage 
                 WHERE table_name = $varchar AND constraint_name = $varchar                            
               )""".query(bool)
      session
        .prepare(query)
        .use(_.unique(table ~ constraintName))
        .recover { case _ => false }
    }
  }

  def tableExists(tableName: String): Boolean = execute[Boolean] {
    Kleisli { session =>
      val query: Query[String, Boolean] =
        sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = $varchar)".query(bool)
      session.prepare(query).use(_.unique(tableName)).recover { case _ => false }
    }
  }

  def viewExists(viewName: String): Boolean = execute[Boolean] {
    Kleisli { session =>
      val query: Query[Void, Boolean] = sql"select exists (select * from #$viewName)".query(bool)
      session.unique(query).recover { case _ => false }
    }
  }

  def dropTable(tableName: String): Unit = execute[Unit] {
    Kleisli { session =>
      val query: Command[Void] = sql"DROP TABLE IF EXISTS #$tableName CASCADE".command
      session.execute(query).void
    }
  }
}
