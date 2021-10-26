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

package io.renku.graph.acceptancetests.db

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.IO._
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.FixedHostPortGenericContainer
import io.renku.db.{DBConfigProvider, SessionResource}
import io.renku.eventlog._
import io.renku.graph.acceptancetests.tooling.TestLogger
import io.renku.graph.model.events.{CommitId, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Id
import natchez.Trace.Implicits.noop
import skunk._
import skunk.implicits._

import scala.collection.immutable

object EventLog extends TypeSerializers {

  private val logger = TestLogger()

  def findEvents(projectId: Id)(implicit ioRuntime: IORuntime): List[(EventId, EventStatus)] = execute { session =>
    val query: Query[projects.Id, (EventId, EventStatus)] =
      sql"""SELECT event_id, status
            FROM event
            WHERE project_id = $projectIdEncoder"""
        .query(eventIdDecoder ~ eventStatusDecoder)
        .map { case id ~ status => (id, status) }
    session.prepare(query).use(_.stream(projectId, 32).compile.toList)
  }

  def findEvents(projectId: Id, status: EventStatus*)(implicit ioRuntime: IORuntime): List[CommitId] = execute {
    session =>
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

  def execute[O](query: Session[IO] => IO[O])(implicit ioRuntime: IORuntime): O =
    sessionResource
      .use(_.useK(Kleisli[IO, Session[IO], O](session => query(session))))
      .unsafeRunSync()

  private def dbConfig(implicit ioRuntime: IORuntime): DBConfigProvider.DBConfig[EventLogDB] =
    new EventLogDbConfigProvider[IO].get().unsafeRunSync()

  private def postgresContainer(implicit ioRuntime: IORuntime) = FixedHostPortGenericContainer(
    imageName = "postgres:11.11-alpine",
    env = immutable.Map("POSTGRES_USER"     -> dbConfig.user.value,
                        "POSTGRES_PASSWORD" -> dbConfig.pass.value,
                        "POSTGRES_DB"       -> dbConfig.name.value
    ),
    exposedPorts = Seq(dbConfig.port.value),
    exposedHostPort = dbConfig.port.value,
    exposedContainerPort = dbConfig.port.value,
    command = Seq(s"-p ${dbConfig.port.value}")
  )

  def startDB()(implicit ioRuntime: IORuntime): IO[Unit] = for {
    _ <- IO(postgresContainer.start())
    _ <- logger.info("event_log DB started")
  } yield ()

  def stopDB()(implicit ioRuntime: IORuntime): IO[Unit] = for {
    _ <- IO(postgresContainer.stop())
    _ <- logger.info("event_log DB stopped")
  } yield ()

  private def sessionResource(implicit ioRuntime: IORuntime): Resource[IO, SessionResource[IO, EventLogDB]] =
    Session
      .pooled(
        host = postgresContainer.host,
        port = postgresContainer.container.getMappedPort(dbConfig.port.value),
        database = dbConfig.name.value,
        user = dbConfig.user.value,
        password = Some(dbConfig.pass.value),
        max = dbConfig.connectionPool.value
      )
      .map(new SessionResource(_))
}
