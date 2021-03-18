/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.acceptancetests.db

import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO}
import ch.datascience.db.{DBConfigProvider, SessionResource}
import ch.datascience.graph.acceptancetests.tooling.TestLogger
import ch.datascience.graph.model.events.{CommitId, EventId, EventStatus}
import ch.datascience.graph.model.projects.Id
import com.dimafeng.testcontainers.{Container, JdbcDatabaseContainer, PostgreSQLContainer}
import doobie.Transactor
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.fragments.in
import io.renku.eventlog._
import org.testcontainers.utility.DockerImageName

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

object EventLog extends TypeSerializers {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
  private val logger = TestLogger()

  def findEvents(projectId: Id, status: EventStatus*): List[CommitId] = execute {
    (fr"""
     SELECT event_id
     FROM event
     WHERE project_id = $projectId AND """ ++ `status IN`(status.toList))
      .query[EventId]
      .to[List]
      .map(_.map(eventId => CommitId(eventId.value)))
  }

  private def `status IN`(status: List[EventStatus]) =
    in(fr"status", NonEmptyList.fromListUnsafe(status))

  def execute[O](query: ConnectionIO[O]): O =
    query
      .transact(transactor.resource)
      .unsafeRunSync()

  private val dbConfig: DBConfigProvider.DBConfig[EventLogDB] =
    new EventLogDbConfigProvider[IO].get().unsafeRunSync()

  private val postgresContainer: Container with JdbcDatabaseContainer = PostgreSQLContainer(
    dockerImageNameOverride = DockerImageName.parse("postgres:9.6.19-alpine"),
    databaseName = "event_log",
    username = dbConfig.user.value,
    password = dbConfig.pass
  )

  lazy val jdbcUrl: String = postgresContainer.jdbcUrl

  def startDB(): IO[Unit] = for {
    _ <- IO(postgresContainer.start())
    _ <- logger.info("event_log DB started")
  } yield ()

  private lazy val transactor: SessionResource[IO, EventLogDB] = DbTransactor[IO, EventLogDB] {
    Transactor.fromDriverManager[IO](
      dbConfig.driver.value,
      postgresContainer.jdbcUrl,
      dbConfig.user.value,
      dbConfig.pass
    )
  }
}
