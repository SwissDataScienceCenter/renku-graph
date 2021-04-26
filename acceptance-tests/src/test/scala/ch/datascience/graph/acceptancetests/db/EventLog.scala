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

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.{Concurrent, ContextShift, IO, Resource}
import ch.datascience.db.{DBConfigProvider, SessionResource}
import ch.datascience.db.implicits._
import ch.datascience.graph.acceptancetests.tooling.TestLogger
import ch.datascience.graph.model.events.{CommitId, EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Id
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import io.renku.eventlog._
import natchez.Trace.Implicits.noop
import skunk._
import skunk.implicits._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

object EventLog extends TypeSerializers {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(global)
  private implicit val concurrent:   Concurrent[IO]   = IO.ioConcurrentEffect
  private val logger = TestLogger()

  def findEvents(projectId: Id): List[(EventId, EventStatus)] = execute { session =>
    val query: Query[projects.Id, (EventId, EventStatus)] =
      sql"""SELECT event_id, status
            FROM event
            WHERE project_id = $projectIdEncoder"""
        .query(eventIdDecoder ~ eventStatusDecoder)
        .map { case id ~ status => (id, status) }
    session.prepare(query).use(_.stream(projectId, 32).compile.toList)
  }

  def findEvents(projectId: Id, status: EventStatus*): List[CommitId] = execute { session =>
    val query: Query[projects.Id, CommitId] =
      sql"""SELECT event_id
            FROM event
            WHERE project_id = $projectIdEncoder AND #${`status IN`(status.toList)}"""
        .query(eventIdDecoder)
        .map(eventId => CommitId(eventId.value))
    session.prepare(query).use(_.stream(projectId, 32).compile.toList)
  }

  private def `status IN`(status: List[EventStatus]) =
    s"status IN (${NonEmptyList.fromListUnsafe(status).map(el => s"'$el'").toList.mkString(",")})"

  def execute[O](query: Session[IO] => IO[O]): O =
    sessionResource
      .use(_.useK(Kleisli[IO, Session[IO], O](session => query(session))))
      .unsafeRunSync()

  private val dbConfig: DBConfigProvider.DBConfig[EventLogDB] =
    new EventLogDbConfigProvider[IO].get().unsafeRunSync()

  private val postgresContainer = FixedHostPortGenericContainer(
    imageName = "postgres:9.6.19-alpine",
    env = Map("POSTGRES_USER"     -> dbConfig.user.value,
              "POSTGRES_PASSWORD" -> dbConfig.pass.value,
              "POSTGRES_DB"       -> dbConfig.name.value
    ),
    exposedPorts = Seq(dbConfig.port.value),
    exposedHostPort = dbConfig.port.value,
    exposedContainerPort = dbConfig.port.value,
    command = Seq(s"-p ${dbConfig.port.value}")
  )

  def startDB(): IO[Unit] = for {
    _ <- IO(postgresContainer.start())
    _ <- logger.info("event_log DB started")
  } yield ()

  private lazy val sessionResource: Resource[IO, SessionResource[IO, EventLogDB]] =
    Session
      .pooled(
        host = postgresContainer.host,
        port = postgresContainer.container.getMappedPort(dbConfig.port.value),
        database = dbConfig.name.value,
        user = dbConfig.user.value,
        password = Some(dbConfig.pass.value),
        max = dbConfig.connectionPool.value,
        readTimeout = dbConfig.maxLifetime,
        writeTimeout = dbConfig.maxLifetime
      )
      .map(new SessionResource(_))
}
